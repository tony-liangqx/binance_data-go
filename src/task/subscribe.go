package task

import (
	"fmt"
	"sync"

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
	// doneCh  chan struct{} // closed when HistorySyncer completes
}

// NewSubscriber creates a new Subscriber instance
func NewSubscriber(storage model.Storage, symbol string, period string) *Subscriber {
	return &Subscriber{
		storage: storage,
		symbol:  symbol,
		period:  period,
	}
}

// GetTimeStamp returns the latest websocket timestamp (thread-safe).
// HistorySyncer calls this to know how far it needs to sync.
func (s *Subscriber) GetTimeStamp() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.timeStamp
}

func (s *Subscriber) IsCaughtUp(currentStart int64) (endTime int64, caughtUp bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	targetTime := s.timeStamp

	// 存在边界问题：进行了+1，所以不存在”=“的情况
	if currentStart > targetTime {
		s.syncing = false
		return 0, true
	}
	return targetTime, false
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
// It clears the syncing flag and closes the doneCh so that the next
// kline event can resume normal processing.
func (s *Subscriber) SyncDone() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncing = false
	// if s.doneCh != nil {
	// 	close(s.doneCh)
	// 	s.doneCh = nil
	// }
	fmt.Println("[ws] history sync completed, resuming normal subscription")
}

// Start implements the Task interface and begins websocket subscription
func (s *Subscriber) Start(timeStamp int64) {
	s.setTimeStamp(timeStamp)
	s.start()
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
// It always updates the timestamp, but only writes to the database
// when a HistorySyncer is NOT actively backfilling.
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

	// Always track the latest websocket position, even during sync
	s.setTimeStamp(point.StartTime)

	// While HistorySyncer is running, subscriber only reads and tracks
	// position — no database writes
	if s.isSyncing() {
		return
	}

	s.processPoint(point)
}

// processPoint handles a closed kline point: checks gap, saves or triggers history sync.
func (s *Subscriber) processPoint(point *model.SpotKlinePoint) {
	// TODO：查询是否导致性能问题？
	lastTime, err := s.storage.GetLastTimeStamp(s.symbol, s.period)
	if err != nil {
		fmt.Printf("failed to get last timestamp: %v\n", err)
		return
	}

	if lastTime > 0 {
		diff := point.StartTime - lastTime
		// For 1m klines, contiguous means exactly 60000ms apart
		// TODO: 改成配置项
		if diff == 60000 {
			// Contiguous data, save directly
			if err := s.storage.Commit(point); err != nil {
				fmt.Printf("failed to commit kline: %v\n", err)
			} else {
				fmt.Printf("[ws] saved kline: %s %s start=%d close=%f\n",
					s.symbol, s.period, point.StartTime, point.Close)
			}
			return
		}

		// Gap detected: start HistorySyncer asynchronously to backfill
		fmt.Printf("[ws] gap detected: last=%d, current=%d, diff=%dms. starting history sync...\n",
			lastTime, point.StartTime, diff)

		// Acquire write lock to set up sync state
		s.mu.Lock()
		if s.syncing {
			s.mu.Unlock()
			return
		}
		s.syncing = true
		s.mu.Unlock()

		// Launch syncer asynchronously — it will backfill from lastTime
		// up to the subscriber's dynamically-advancing timestamp, then
		// call SyncDone to hand write control back to the subscriber.
		syncer := NewHistorySyncer(s.storage, s.symbol, s.period, lastTime, s)
		go syncer.Sync()
		// Note: the trigger point is NOT committed here. The syncer
		// will fetch it as part of the historical backfill when it
		// syncs up to s.GetTimeStamp().
	} else {
		// No existing data, save directly
		if err := s.storage.Commit(point); err != nil {
			fmt.Printf("failed to commit first kline: %v\n", err)
		} else {
			fmt.Printf("[ws] saved first kline: %s %s start=%d close=%f\n",
				s.symbol, s.period, point.StartTime, point.Close)
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

// Ensure compile-time interface compliance
var _ Task = (*Subscriber)(nil)
