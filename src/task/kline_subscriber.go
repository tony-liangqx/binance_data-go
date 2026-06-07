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

	// aggregator is the volatility aggregator that processes incoming kline points
	aggregators []*model.VolatilityDataWriter

	// buffer holds kline points that arrived via websocket while a
	// HistorySyncer was backfilling. They are drained in SyncDone,
	// after the syncer's data has been committed to storage.
	buffer []*model.SpotKlinePoint

	// pointChan is an optional channel for publishing processed points
	// to external consumers (e.g., PubSubService for MQTT aggregation).
	pointChan chan<- *model.AggregatedKline
}

// NewSubscriber creates a new Subscriber instance
func NewSubscriber(storage model.Storage, symbol string, period string) *Subscriber {
	aggregators := []*model.VolatilityDataWriter{
		model.NewVolatilityDataWriter(symbol, 0.5, storage),
		model.NewVolatilityDataWriter(symbol, 1, storage),
		model.NewVolatilityDataWriter(symbol, 2, storage),
	}
	return &Subscriber{
		storage:     storage,
		symbol:      symbol,
		period:      period,
		aggregators: aggregators,
	}
}

// SetPointChan sets the channel for publishing processed points to external consumers.
func (s *Subscriber) SetPointChan(ch chan<- *model.AggregatedKline) {
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

// alignWithKline backfills the volatility aggregation table (agg_binance_spot_kline) with
// any klines that were saved to binance_spot_kline but not yet aggregated.
//
// It works in three steps:
//  1. Get the latest CloseTime from agg_binance_spot_kline (the last aggregated window's end).
//  2. Query binance_spot_kline for records where start_time > that CloseTime.
//  3. Run each record through s.aggregatePoint so the VolatilityDataWriters rebuild their windows.
//
// This ensures that after a restart, volatility aggregation resumes from where it left off.
func (s *Subscriber) alignWithKline() {
	fmt.Println("start align with kline data task")
	defer func() {
		fmt.Println("align with kline data task completed")
	}()

	// 1. 获取AggBinanceSpotKline数据库中最后一条kline记录的Close时间戳
	records := make([]*model.AggBinanceSpotKline, 0)
	err := s.storage.GetDB().Raw(
		`SELECT *
    FROM agg_binance_spot_kline
    WHERE symbol = ?
    AND (volatility, close_time) IN (
        SELECT volatility, MAX(close_time)
        FROM agg_binance_spot_kline
        WHERE symbol = ?
        GROUP BY volatility
    )`, s.symbol, s.symbol,
	).Scan(&records).Error
	if err != nil {
		fmt.Printf("[alignWithKline] %s %s failed to get last agg close_time: %v\n", s.symbol, s.period, err)
		panic(err)
	}

	for _, record := range records {
		symbol := record.Symbol
		lastCloseTime := record.CloseTime
		// 2. 获取BinanceSpotKline数据库中大于获取的Close时间戳的全部记录
		var klines []model.BinanceSpotKline
		err = s.storage.GetDB().Raw(
			"SELECT * FROM binance_spot_kline WHERE symbol = ? AND start_time > ? ORDER BY start_time ASC",
			symbol,
			lastCloseTime,
		).Scan(&klines).Error
		if err != nil {
			fmt.Printf("[alignWithKline] %s %s failed to query klines: %v\n", s.symbol, s.period, err)
			panic(err)
		}

		fmt.Printf("[alignWithKline] %s %s found %d klines to align\n", s.symbol, s.period, len(klines))

		// 3. 全部记录按时间顺序交给aggregatePoint处理
		for _, kline := range klines {
			point := &model.SpotKlinePoint{
				Symbol:           kline.Symbol,
				Period:           kline.Period,
				StartTime:        kline.StartTime,
				DateTime:         int64(kline.DateTime),
				Open:             kline.Open,
				High:             kline.High,
				Low:              kline.Low,
				Close:            kline.Close,
				Volume:           kline.Volume,
				QuoteAssetVolume: kline.QuoteAssetVolume,
				Trades:           kline.Trades,
				CloseTime:        kline.CloseTime,
			}
			_, err := s.aggregatePoint(point)
			if err != nil {
				fmt.Printf("[alignWithKline] %s %s failed to aggregate point: start_time=%d, err=%v\n",
					s.symbol, s.period, point.StartTime, err)
				panic(err)
			}
		}
		fmt.Printf("[alignWithKline] %s %s alignment completed, processed %d klines\n",
			s.symbol, s.period, len(klines))
	}
}

// Start implements the Task interface and begins websocket subscription
func (s *Subscriber) Start(timeStamp int64) {
	// 对齐kline记录与volality数据库记录
	s.setTimeStamp(timeStamp)
	s.alignWithKline()
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

func (s *Subscriber) HandleKline(kline *binance.WsKline) {
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

// handleKline processes each incoming kline event.
// It always updates the timestamp. If a HistorySyncer is actively backfilling,
// the point is buffered and replayed after sync completes — preventing data loss
// when the syncer's targetTime was captured before this point arrived.
func (s *Subscriber) handleKline(event *binance.WsKlineEvent) {
	kline := event.Kline

	s.HandleKline(&kline)
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
			return s.savePoint(point)
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
		syncer := NewHistorySyncer(s.storage, s.symbol, s.period, lastTime, s, s.savePoint)
		go syncer.Sync()
		// Note: the trigger point is NOT committed here. The syncer
		// will fetch it as part of the historical backfill when it
		// syncs up to s.GetTimeStamp().
		return nil
	} else {
		// No existing data, save directly
		return s.savePoint(point)
	}
}

// publish出去的数据兼容“1m数据格式”和“合并volatility数据”
func (s *Subscriber) aggregatePoint(point *model.SpotKlinePoint) ([]*model.AggregatedKline, error) {
	points := make([]*model.AggregatedKline, 0, len(s.aggregators)+1)
	for _, aggregator := range s.aggregators {
		agg, err := aggregator.Add(point)
		if err != nil {
			fmt.Printf("[volatility_data_writer(%s %s)] failed to add point to aggregator: %v\n", aggregator.Symbol(), aggregator.Volatility(), err)
			return nil, err
		}
		if agg != nil {
			points = append(points, agg)
		}
	}
	return points, nil
}

func (s *Subscriber) savePoint(point *model.SpotKlinePoint) error {
	{
		// Commit会合并volatility数据
		points, err := s.aggregatePoint(point)
		if err != nil {
			fmt.Printf("[subscriber] aggregator error: %v\n", err)
			// TODO:: 未来解决
			panic(err)
		}

		if err := s.storage.Commit(point); err != nil {
			fmt.Printf("[subscriber] failed to commit kline error: %v\n", err)
			// TODO:: 未来解决
			panic(err)
		}

		fmt.Printf("[subscriber] saved kline: %s %s start=%d close=%f\n",
			s.symbol, s.period, point.StartTime, point.Close)

		lastPoint := &model.AggregatedKline{
			Symbol:           point.Symbol,
			Period:           point.Period,
			StartTime:        point.StartTime,
			Open:             point.Open,
			High:             point.High,
			Low:              point.Low,
			Close:            point.Close,
			Volume:           point.Volume,
			CloseTime:        point.CloseTime,
			QuoteAssetVolume: point.QuoteAssetVolume,
			Trades:           point.Trades,
		}

		points = append(points, lastPoint)
		for _, point := range points {
			s.publishPoint(point)
		}
		return nil
	}
}

// publishPoint sends the point to the external channel if one is configured.
// Sends are non-blocking to avoid slowing down the subscriber.
func (s *Subscriber) publishPoint(point *model.AggregatedKline) {
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
