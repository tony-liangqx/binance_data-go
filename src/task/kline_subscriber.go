package task

import (
	"fmt"
	"log"
	"sync"
	"time"

	"binance.data.sync/src/model"

	"github.com/adshao/go-binance/v2/futures"
)

// Subscriber subscribes to Binance websocket kline data,
// filters closed klines, detects data gaps, and triggers HistorySyncer when needed.
//
// Multiple Subscriber instances share a single WebSocket connection via
// KlineConnection, which dispatches incoming kline events to the correct
// Subscriber based on the symbol in the event data.
type Subscriber struct {
	timeStamp int64
	storage   model.Storage
	symbol    string
	period    string
	mu        sync.RWMutex

	// conn is the shared KlineConnection that this subscriber registers with.
	// When set, the subscriber does not create its own WebSocket connection;
	// instead it receives kline events dispatched by the shared connection.
	conn *KlineConnection

	// Sync coordination — used when HistorySyncer is backfilling data
	syncing bool

	// aggregator is the volatility aggregator that processes incoming kline points
	aggregators []*model.GridVolatilityDataWriter

	// buffer holds kline points that arrived via websocket while a
	// HistorySyncer was backfilling. They are drained in SyncDone,
	// after the syncer's data has been committed to storage.
	buffer []*model.FutureKlinePoint

	// pointChan is an optional channel for publishing processed points
	// to external consumers (e.g., PubSubService for MQTT aggregation).
	pointChan chan<- *model.AggregatedFutureKline

	alignEventC       chan bool
	loadHistoryEventC chan bool
}

// NewSubscriber creates a new Subscriber instance
func NewSubscriber(storage model.Storage, symbol string, period string) *Subscriber {
	aggregators := []*model.GridVolatilityDataWriter{
		model.NewGridVolatilityDataWriter(1, storage),
		// model.NewVolatilityDataWriter(symbol, 0.5, storage),
		// model.NewVolatilityDataWriter(symbol, 1, storage),
		// model.NewVolatilityDataWriter(symbol, 2, storage),
	}
	return &Subscriber{
		storage:     storage,
		symbol:      symbol,
		period:      period,
		aggregators: aggregators,
	}
}

// SetPointChan sets the channel for publishing processed points to external consumers.
func (s *Subscriber) SetPointChan(ch chan<- *model.AggregatedFutureKline) {
	s.pointChan = ch
}

func (s *Subscriber) SetEventChan(alignEvent, loadHistoryEvent chan bool) {
	s.alignEventC = alignEvent
	s.loadHistoryEventC = loadHistoryEvent
}

// SetKlineConnection sets the shared KlineConnection for this subscriber.
// When set, the subscriber will register with this connection instead of
// creating its own individual WebSocket connection.
func (s *Subscriber) SetKlineConnection(conn *KlineConnection) {
	s.conn = conn
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

	log.Printf("[ws] %s %s history sync completed, resuming normal subscription(at %d)\n", s.symbol, s.period, s.timeStamp)

	// Process buffered points in order. These points arrived via websocket
	// while the syncer was running. Many may already be committed by the
	// syncer — processPoint handles that by checking storage's lastTime.
	for _, point := range buffer {
		s.processPoint(point)
	}
}

// alignWithKline backfills the volatility aggregation table (agg_binance_futures_kline) with
// any klines that were saved to binance_futures_kline but not yet aggregated.
//
// It works in three steps:
//  1. For EACH configured volatility, get the last CloseTime from agg_binance_futures_kline.
//     For volatilities with NO records (missing), find the earliest start_time from binance_futures_kline.
//  2. Query binance_futures_kline for records where start_time > each volatility's cutoff,
//     using a single pass with cursor pagination.
//  3. Run each record through each aggregator's Add method individually, skipping
//     aggregators whose cutoff has not been passed (to prevent re-aggregation).
//
// This ensures that after a restart, volatility aggregation resumes from where it left off.
func (s *Subscriber) alignWithKline() {
	log.Println("start align with kline data task")
	defer func() {
		log.Println("align with kline data task completed")
	}()

	// 1. 获取每个volatility的最后一条记录的close_time
	//    - 有历史记录: 从agg_binance_futures_kline取最后一条close_time
	//    - 缺失记录: 从binance_futures_kline最小的start_time开始
	type volBoundary struct {
		lastCloseTime int64 // 已有volatility的最后close_time, 缺失时为0
		exists        bool  // 该volatility是否已有历史记录
	}
	volBoundaries := make(map[string]*volBoundary)

	// 查询每个volatility的最后一条agg记录
	for _, aggregator := range s.aggregators {
		vol := aggregator.Volatility()
		var last model.AggBinanceFutureKline
		err := s.storage.GetDB().Raw(
			"SELECT * FROM agg_binance_futures_kline WHERE symbol = ? AND period = ? AND volatility = ? ORDER BY start_time DESC LIMIT 1",
			s.symbol, s.period, vol,
		).Scan(&last).Error
		if err != nil {
			log.Printf("[alignWithKline] %s %s failed to query last agg close_time for volatility=%s: %v\n",
				s.symbol, s.period, vol, err)
			panic(err)
		}
		if last.StartTime != 0 {
			volBoundaries[vol] = &volBoundary{lastCloseTime: last.CloseTime, exists: true}
			log.Printf("[alignWithKline] volatility %s last close_time: %d\n", vol, last.CloseTime)
		} else {
			// 1. 缺失的volatility从BinanceFutureKline表中最小的start_time开始
			var minStartTime int64
			err := s.storage.GetDB().Raw(
				"SELECT MIN(start_time) FROM binance_futures_kline WHERE symbol = ? AND period = ?",
				s.symbol, s.period,
			).Scan(&minStartTime).Error
			if err != nil {
				log.Printf("[alignWithKline] %s %s failed to query min start_time: %v\n",
					s.symbol, s.period, err)
				panic(err)
			}
			// 使用minStartTime-1确保能获取到start_time=minStartTime的记录(因为查询条件是 >)
			fromTime := minStartTime - 1
			if fromTime < 0 {
				fromTime = 0
			}
			volBoundaries[vol] = &volBoundary{lastCloseTime: fromTime, exists: false}
			log.Printf("[alignWithKline] volatility %s has no history, starting from start_time: %d\n",
				vol, minStartTime)
		}
	}

	// 确定全局起始时间: 使用所有volatility中最小的lastCloseTime, 确保不遗漏任何数据
	var globalFromTime int64
	first := true
	for _, vb := range volBoundaries {
		if first || vb.lastCloseTime < globalFromTime {
			globalFromTime = vb.lastCloseTime
			first = false
		}
	}

	if first {
		// 没有配置任何aggregator, 直接返回
		log.Printf("[alignWithKline] %s %s no aggregators configured, skipping\n", s.symbol, s.period)
		return
	}

	// 3. 使用游标(cursor)方式分页获取数据
	const pageSize = 5000
	var lastStartTime int64 = globalFromTime
	totalProcessed := 0

	for {
		var klines []model.BinanceFutureKline
		err := s.storage.GetDB().Raw(
			"SELECT * FROM binance_futures_kline WHERE symbol = ? AND start_time > ? ORDER BY start_time ASC LIMIT ?",
			s.symbol, lastStartTime, pageSize,
		).Scan(&klines).Error
		if err != nil {
			log.Printf("[alignWithKline] %s %s failed to query klines: %v\n", s.symbol, s.period, err)
			panic(err)
		}

		if len(klines) == 0 {
			break
		}

		for _, kline := range klines {
			point := &model.FutureKlinePoint{
				Symbol:                   kline.Symbol,
				Period:                   kline.Period,
				StartTime:                kline.StartTime,
				DateTime:                 int64(kline.DateTime),
				Open:                     kline.Open,
				High:                     kline.High,
				Low:                      kline.Low,
				Close:                    kline.Close,
				Volume:                   kline.Volume,
				QuoteAssetVolume:         kline.QuoteAssetVolume,
				Trades:                   kline.Trades,
				CloseTime:                kline.CloseTime,
				TakerBuyBaseAssetVolume:  kline.TakerBuyBaseAssetVolume,
				TakerBuyQuoteAssetVolume: kline.TakerBuyQuoteAssetVolume,
			}

			// 2. 有历史volatility信息的情况: 只处理start_time大于该volatility的lastCloseTime的记录
			//    缺失volatility的情况: 全部记录都需要处理(lastCloseTime为0或最小值)
			for _, aggregator := range s.aggregators {
				vb := volBoundaries[aggregator.Volatility()]
				if vb.exists && point.StartTime <= vb.lastCloseTime {
					// 该kline已被该volatility处理过, 跳过
					continue
				}
				_, err := aggregator.Add(point)
				if err != nil {
					log.Printf("[alignWithKline] %s %s failed to aggregate point for volatility=%s: start_time=%d, err=%v\n",
						s.symbol, s.period, aggregator.Volatility(), point.StartTime, err)
					panic(err)
				}
				// if agg != nil {
				// 	agg.Kind = "volatility"
				// 	s.publishPoint(agg)
				// }
			}
		}

		totalProcessed += len(klines)
		lastStartTime = klines[len(klines)-1].StartTime
		log.Printf("[alignWithKline] %s %s processed %d klines (batch, total=%d)...\n",
			s.symbol, s.period, len(klines), totalProcessed)

		if len(klines) < pageSize {
			break
		}
	}

	log.Printf("[alignWithKline] %s %s alignment completed, total processed %d klines\n",
		s.symbol, s.period, totalProcessed)
}

// Start implements the Task interface and begins websocket subscription
func (s *Subscriber) Start(timeStamp int64) {
	// 对齐kline记录与volality数据库记录
	s.setTimeStamp(timeStamp)
	s.alignWithKline()
	// 通知已经完成对齐任务
	s.alignEventC <- true
	// 等待历史数据加载完成
	<-s.loadHistoryEventC
	// 重试
	for {
		s.start()
		time.Sleep(time.Second * 10)
	}
}

// start registers this subscriber with the shared KlineConnection.
// If no shared connection is configured (e.g. in tests), it falls back to
// creating its own individual WebSocket connection for backward compatibility.
func (s *Subscriber) start() {
	if s.conn != nil {
		s.conn.Register(s)
		// Block forever — HandleKline will be called by the shared
		// connection's dispatcher when kline events arrive for this symbol.
		select {}
	}

	// Fallback: individual connection (used in tests)
	doneC, stopC, err := futures.WsKlineServe(s.symbol, s.period, s.handleKline, s.handleError)
	if err != nil {
		log.Printf("failed to start kline websocket: %v\n", err)
		return
	}

	log.Printf("subscriber started: symbol=%s, period=%s\n", s.symbol, s.period)

	<-doneC
	log.Println("subscriber websocket connection closed")
	_ = stopC
}

func (s *Subscriber) HandleKline(kline *futures.WsKline) {
	// Only process closed (finished) klines
	if !kline.IsFinal {
		return
	}

	point := &model.FutureKlinePoint{
		Symbol:                   s.symbol,
		Period:                   s.period,
		StartTime:                kline.StartTime,
		DateTime:                 kline.StartTime,
		Open:                     mustParseFloat(kline.Open),
		High:                     mustParseFloat(kline.High),
		Low:                      mustParseFloat(kline.Low),
		Close:                    mustParseFloat(kline.Close),
		Volume:                   mustParseFloat(kline.Volume),
		CloseTime:                kline.EndTime,
		QuoteAssetVolume:         mustParseFloat(kline.QuoteVolume),
		TakerBuyBaseAssetVolume:  mustParseFloat(kline.ActiveBuyVolume),
		TakerBuyQuoteAssetVolume: mustParseFloat(kline.ActiveBuyQuoteVolume),
		Trades:                   uint32(kline.TradeNum),
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
func (s *Subscriber) handleKline(event *futures.WsKlineEvent) {
	kline := event.Kline

	s.HandleKline(&kline)
}

// processPoint handles a closed kline point: checks gap, saves or triggers history sync.
func (s *Subscriber) processPoint(point *model.FutureKlinePoint) error {
	// TODO：查询是否导致性能问题？
	lastTime, err := s.storage.GetLastTimeStamp(s.symbol, s.period)
	if err != nil {
		log.Printf("failed to get last timestamp: %v\n", err)
		return err
	}

	// Point was already committed by a HistorySyncer during backfill.
	// This happens when SyncDone drains buffered points that the syncer
	// already wrote. Skip to avoid duplicate-key errors (unique index on
	// symbol+period+start_time) and wasted work.
	if lastTime > 0 && point.StartTime <= lastTime {
		log.Printf("[ws] skip already synced kline: %s %s start=%d\n",
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
		log.Printf("[ws] gap detected: last=%d, current=%d, diff=%dms. starting history sync...\n",
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
func (s *Subscriber) aggregatePoint(point *model.FutureKlinePoint) ([]*model.AggregatedFutureKline, error) {
	points := make([]*model.AggregatedFutureKline, 0, len(s.aggregators)+1)
	for _, aggregator := range s.aggregators {
		agg, err := aggregator.Add(point)
		if err != nil {
			log.Printf("[volatility_data_writer(%s %s)] failed to add point to aggregator: %v\n", aggregator.Symbol(), aggregator.Volatility(), err)
			return nil, err
		}
		if agg != nil {
			agg.Kind = "volatility"
			points = append(points, agg)
		}
	}
	return points, nil
}

func (s *Subscriber) savePoint(point *model.FutureKlinePoint) error {
	{
		// Commit会合并volatility数据
		needPubPoints, err := s.aggregatePoint(point)
		if err != nil {
			log.Printf("[subscriber] aggregator error: %v\n", err)
			// TODO:: 未来解决
			panic(err)
		}

		if err := s.storage.Commit(point); err != nil {
			log.Printf("[subscriber] failed to commit kline error: %v\n", err)
			// TODO:: 未来解决
			panic(err)
		}

		log.Printf("[subscriber] saved kline: %s %s start=%d close=%f\n",
			s.symbol, s.period, point.StartTime, point.Close)

		kline := &model.AggregatedFutureKline{
			Symbol:                   point.Symbol,
			Period:                   point.Period,
			Kind:                     "kline",
			StartTime:                point.StartTime,
			Open:                     point.Open,
			High:                     point.High,
			Low:                      point.Low,
			Close:                    point.Close,
			Volume:                   point.Volume,
			CloseTime:                point.CloseTime,
			QuoteAssetVolume:         point.QuoteAssetVolume,
			Trades:                   point.Trades,
			TakerBuyBaseAssetVolume:  point.TakerBuyBaseAssetVolume,
			TakerBuyQuoteAssetVolume: point.TakerBuyQuoteAssetVolume,
		}

		needPubPoints = append(needPubPoints, kline)
		for _, point := range needPubPoints {
			s.publishPoint(point)
		}
		return nil
	}
}

// publishPoint sends the point to the external channel if one is configured.
// Sends are non-blocking to avoid slowing down the subscriber.
func (s *Subscriber) publishPoint(point *model.AggregatedFutureKline) {
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
	log.Printf("[ws] websocket error: %v\n", err)
}

// mustParseFloat parses a string to float64, panics on failure
func mustParseFloat(s string) float64 {
	v, err := parseFloat(s)
	if err != nil {
		log.Printf("warning: failed to parse float %q: %v\n", s, err)
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
func (s *Subscriber) bufferPoint(point *model.FutureKlinePoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buffer = append(s.buffer, point)
}

// Ensure compile-time interface compliance
var _ Task = (*Subscriber)(nil)
