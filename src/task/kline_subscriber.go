package task

import (
	"fmt"
	"sync"
	"time"

	"binance.data.sync/src/model"

	"github.com/adshao/go-binance/v2"
)

// Subscriber subscribes to Binance websocket kline data,
// filters closed klines, detects data gaps, and triggers HistorySyncer when needed.
type Subscriber struct {
	timeStamp int64
	storage   model.Storage
	symbol    string
	period    string
	mu        sync.RWMutex

	// Sync coordination — used when HistorySyncer is backfilling data
	syncing bool

	// buffer holds kline points that arrived via websocket while a
	// HistorySyncer was backfilling. They are drained in SyncDone,
	// after the syncer's data has been committed to storage.
	buffer []*model.SpotKlinePoint

	// pointChan is an optional channel for publishing processed points
	// to external consumers (e.g., PubSubService for MQTT aggregation).
	pointChan chan<- *model.SpotKlinePoint
}

// NewSubscriber creates a new Subscriber instance
func NewSubscriber(storage model.Storage, symbol string, period string) *Subscriber {
	return &Subscriber{
		storage: storage,
		symbol:  symbol,
		period:  period,
	}
}

// SetPointChan sets the channel for publishing processed points to external consumers.
func (s *Subscriber) SetPointChan(ch chan<- *model.SpotKlinePoint) {
	s.pointChan = ch
}

// GetTimeStamp returns the latest websocket timestamp (thread-safe).
// HistorySyncer calls this to know how far it needs to sync.
func (s *Subscriber) GetTimeStamp() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.timeStamp
}

// setTimeStamp safely updates the timestamp (thread-safe).
func (s *Subscriber) setTimeStamp(ts int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.timeStamp = ts
}

// isSyncing returns whether a HistorySyncer is currently running.
func (s *Subscriber) isSyncing() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.syncing
}

// SyncDone is called by HistorySyncer when it finishes backfilling.
// It clears the syncing flag, then drains any points that were buffered
// during the sync. This ensures websocket data that arrived during backfill
// is not lost — even if the syncer's REST fetch didn't cover it.
func (s *Subscriber) SyncDone() {
	s.mu.Lock()
	s.syncing = false
	buffer := s.buffer
	s.buffer = nil
	s.mu.Unlock()

	fmt.Printf("[ws] %s %s history sync completed, resuming normal subscription(at %d)\n", s.symbol, s.period, s.timeStamp)

	// Process buffered points in order. These points arrived via websocket
	// while the syncer was running. Many may already be committed by the
	// syncer — processPoint handles that by checking storage's lastTime.
	for _, point := range buffer {
		s.processPoint(point)
	}
}

// Start implements the Task interface and begins websocket subscription
func (s *Subscriber) Start(timeStamp int64) {
	s.setTimeStamp(timeStamp)
	// 重试
	for {
		s.start()
		time.Sleep(time.Second * 10)
	}
}

// start begins the actual websocket kline subscription loop
func (s *Subscriber) start() {
	doneC, stopC, err := binance.WsKlineServe(s.symbol, s.period, s.handleKline, s.handleError)
	if err != nil {
		fmt.Printf("failed to start kline websocket: %v\n", err)
		return
	}

	fmt.Printf("subscriber started: symbol=%s, period=%s\n", s.symbol, s.period)

	// Block until the connection is closed
	<-doneC
	fmt.Println("subscriber websocket connection closed")
	_ = stopC
}

// handleKline processes each incoming kline event.
// It always updates the timestamp. If a HistorySyncer is actively backfilling,
// the point is buffered and replayed after sync completes — preventing data loss
// when the syncer's targetTime was captured before this point arrived.
func (s *Subscriber) handleKline(event *binance.WsKlineEvent) {
	kline := event.Kline

	// Only process closed (finished) klines
	if !kline.IsFinal {
		return
	}

	point := &model.SpotKlinePoint{
		Symbol:           s.symbol,
		Period:           s.period,
		StartTime:        kline.StartTime,
		DateTime:         kline.StartTime,
		Open:             mustParseFloat(kline.Open),
		High:             mustParseFloat(kline.High),
		Low:              mustParseFloat(kline.Low),
		Close:            mustParseFloat(kline.Close),
		Volume:           mustParseFloat(kline.Volume),
		CloseTime:        kline.EndTime,
		QuoteAssetVolume: mustParseFloat(kline.QuoteVolume),
		Trades:           uint32(kline.TradeNum),
	}

	// Always track the latest websocket position, even during sync.
	// HistorySyncer reads this via GetTimeStamp()/IsCaughtUp() to know
	// how far it needs to backfill.
	s.setTimeStamp(point.StartTime)

	// While HistorySyncer is running, buffer the point instead of dropping it.
	// The syncer's targetTime is captured at each IsCaughtUp call, so a point
	// that arrives right after that call won't be covered by the REST fetch.
	// Buffering ensures no data loss regardless of timing.
	if s.isSyncing() {
		s.bufferPoint(point)
		return
	}

	err := s.processPoint(point)
	if err != nil {
		// 数据无法写入，panic重启
		panic(err)
	}
}

// processPoint handles a closed kline point: checks gap, saves or triggers history sync.
func (s *Subscriber) processPoint(point *model.SpotKlinePoint) error {
	// TODO：查询是否导致性能问题？
	lastTime, err := s.storage.GetLastTimeStamp(s.symbol, s.period)
	if err != nil {
		fmt.Printf("failed to get last timestamp: %v\n", err)
		return err
	}

	// Point was already committed by a HistorySyncer during backfill.
	// This happens when SyncDone drains buffered points that the syncer
	// already wrote. Skip to avoid duplicate-key errors (unique index on
	// symbol+period+start_time) and wasted work.
	if lastTime > 0 && point.StartTime <= lastTime {
		fmt.Printf("[ws] skip already synced kline: %s %s start=%d\n",
			s.symbol, s.period, point.StartTime)
		return nil
	}

	if lastTime > 0 {
		diff := point.StartTime - lastTime
		// For 1m klines, contiguous means exactly 60000ms apart
		// TODO: 改成配置项
		if diff == 60000 {
			// Contiguous data, save directly
			if err := s.storage.Commit(point); err != nil {
				fmt.Printf("failed to commit kline: %v\n", err)
				return err
			} else {
				fmt.Printf("[ws] saved kline: %s %s start=%d close=%f\n",
					s.symbol, s.period, point.StartTime, point.Close)
			}
			s.publishPoint(point)
			return nil
		}

		// Gap detected: start HistorySyncer asynchronously to backfill
		fmt.Printf("[ws] gap detected: last=%d, current=%d, diff=%dms. starting history sync...\n",
			lastTime, point.StartTime, diff)

		// Acquire write lock to set up sync state
		s.mu.Lock()
		if s.syncing {
			s.mu.Unlock()
			return nil
		}
		s.syncing = true
		s.mu.Unlock()

		// Launch syncer asynchronously — it will backfill from lastTime
		// up to the subscriber's dynamically-advancing timestamp, then
		// call SyncDone to hand write control back to the subscriber.
		syncer := NewHistorySyncer(s.storage, s.symbol, s.period, lastTime, s, s.pointChan)
		go syncer.Sync()
		// Note: the trigger point is NOT committed here. The syncer
		// will fetch it as part of the historical backfill when it
		// syncs up to s.GetTimeStamp().
		return nil
	} else {
		// No existing data, save directly
		if err := s.storage.Commit(point); err != nil {
			fmt.Printf("failed to commit first kline: %v\n", err)
			return err
		}
		fmt.Printf("[ws] saved first kline: %s %s start=%d close=%f\n",
			s.symbol, s.period, point.StartTime, point.Close)

		s.publishPoint(point)
		return nil
	}
}

// publishPoint sends the point to the external channel if one is configured.
// Sends are non-blocking to avoid slowing down the subscriber.
func (s *Subscriber) publishPoint(point *model.SpotKlinePoint) {
	if s.pointChan != nil {
		select {
		case s.pointChan <- point:
		default:
			// Channel full, skip to avoid blocking the subscriber
		}
	}
}

// handleError handles websocket errors
func (s *Subscriber) handleError(err error) {
	fmt.Printf("[ws] websocket error: %v\n", err)
}

// mustParseFloat parses a string to float64, panics on failure
func mustParseFloat(s string) float64 {
	v, err := parseFloat(s)
	if err != nil {
		fmt.Printf("warning: failed to parse float %q: %v\n", s, err)
		return 0
	}
	return v
}

// parseFloat converts a string to float64, using decimal for precision
func parseFloat(s string) (float64, error) {
	var v float64
	_, err := fmt.Sscanf(s, "%f", &v)
	return v, err
}

// bufferPoint safely appends a kline point to the buffer during sync.
// Caller must hold no lock, or at most a read lock — this acquires the write lock.
func (s *Subscriber) bufferPoint(point *model.SpotKlinePoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buffer = append(s.buffer, point)
}

// Ensure compile-time interface compliance
var _ Task = (*Subscriber)(nil)
