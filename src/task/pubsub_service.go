package task

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"binance.data.sync/src/model"
)

const (
	// Default MQTT broker address
	defaultMQTTBroker = "tcp://127.0.0.1:1883"

	// MQTT topic prefix for aggregated kline data
	mqttTopicPrefix           = "binance/aggregated"
	mqttVolatilityTopicPrefix = "binance/volatility"

	// Default SMA window size for indicators created on subscription
	defaultSMAWindow = 14
)

// PubSubService receives SpotKlinePoint data, routes it to per-subscription
// symbolAggregators, publishes aggregated results to MQTT, and runs
// indicator calculations.
//
// Latest 1m data per symbol is cached in memory for fast retrieval.
// Aggregators and indicators are created on-demand via Subscribe().
type PubSubService struct {
	broker   string
	clientID string

	// PointChan receives raw 1m kline points from Subscribers
	PointChan chan *model.AggregatedFutureKline

	// storage persists aggregated kline data to the database
	storage model.Storage

	// aggregators maps "symbol:period" -> ISymbolAggregator
	// Created on-demand when users subscribe to aggregated streams
	aggregators  map[string]ISymbolAggregator
	subRefCounts map[string]int // reference count per "symbol:period"
	mu           sync.RWMutex

	// latestPoints caches the latest 1m point per symbol (key = symbol)
	latestPoints map[string]*model.AggregatedFutureKline
	latestMu     sync.RWMutex

	// localSubscribers maps topic -> set of channels for in-process subscribers.
	// This allows WebSocketService (and other in-process consumers) to receive
	// aggregated data directly without going through MQTT.
	localSubscribers map[string]map[chan []byte]struct{}
	localMu          sync.RWMutex

	// 通知事件通道
	AlignEventC       chan bool
	LoadHistoryEventC chan bool
	eventCount        int

	// 网络对象
	conn *KlineConnection
}

// NewPubSubService creates a new PubSubService with default MQTT broker settings.
func NewPubSubService(eventCount int) *PubSubService {
	return NewPubSubServiceWithBroker(defaultMQTTBroker, eventCount)
}

// NewPubSubServiceWithBroker creates a new PubSubService with a custom MQTT broker address.
func NewPubSubServiceWithBroker(broker string, eventCount int) *PubSubService {
	clientID := fmt.Sprintf("binance_pubsub_%d", time.Now().UnixNano())

	return &PubSubService{
		broker:            broker,
		clientID:          clientID,
		PointChan:         make(chan *model.AggregatedFutureKline, 1024),
		aggregators:       make(map[string]ISymbolAggregator),
		subRefCounts:      make(map[string]int),
		latestPoints:      make(map[string]*model.AggregatedFutureKline),
		AlignEventC:       make(chan bool),
		LoadHistoryEventC: make(chan bool),
		eventCount:        eventCount,
		localSubscribers:  make(map[string]map[chan []byte]struct{}),
	}
}

// SetStorage sets the storage backend for persisting aggregated kline data.
func (s *PubSubService) SetStorage(storage model.Storage) {
	s.storage = storage
}

func (s *PubSubService) SetKlineConnection(conn *KlineConnection) {
	s.conn = conn
}

// SubscribeLocal registers a channel to receive raw JSON payloads for a given topic.
// This allows in-process consumers (e.g. WebSocketService) to receive aggregated
// data directly without going through MQTT.
func (s *PubSubService) SubscribeLocal(topic string, ch chan []byte) {
	s.localMu.Lock()
	defer s.localMu.Unlock()

	if _, ok := s.localSubscribers[topic]; !ok {
		s.localSubscribers[topic] = make(map[chan []byte]struct{})
	}
	s.localSubscribers[topic][ch] = struct{}{}
	log.Printf("[pubsub] local subscriber added for topic %s\n", topic)
}

// UnsubscribeLocal removes a channel from the local subscribers for a given topic.
func (s *PubSubService) UnsubscribeLocal(topic string, ch chan []byte) {
	s.localMu.Lock()
	defer s.localMu.Unlock()

	if subscribers, ok := s.localSubscribers[topic]; ok {
		delete(subscribers, ch)
		if len(subscribers) == 0 {
			delete(s.localSubscribers, topic)
		}
		log.Printf("[pubsub] local subscriber removed for topic %s\n", topic)
	}
}

// Subscribe creates a symbolAggregator for the given (symbol, period) pair
// with default indicators. This is called when a user subscribes to an
// aggregated stream (e.g. "BTCUSDT@kline_1m/ETHUSDT@kline_1m").
//
// It maintains a reference counter for each (symbol, period). The aggregator
// is only created on the first subscription and removed when the last
// subscriber calls Unsubscribe.
func (s *PubSubService) Subscribe(symbol, kind string) model.AggregatedFutureKline {
	period := "1m"
	key := fmt.Sprintf("%s_%s_%s", symbol, kind, period)

	s.mu.Lock()
	defer s.mu.Unlock()

	var point model.AggregatedFutureKline

	if aggregator, ok := s.aggregators[key]; ok {
		s.subRefCounts[key]++
		log.Printf("[pubsub] subscribe ref++ for %s/%s (refCount=%d, indicators=%d)\n",
			symbol, period, s.subRefCounts[key], len(aggregator.Indicators()))
		switch kind {
		case "volatility":
			point = s.GetLatestPoint(symbol, kind, period)
			point.Period = ""
		default:
			point = s.GetLatestPoint(symbol, kind, period)
			point.Period = period
			aggregator.SetFirstPoint(&point)
		}
		return point
	}

	var aggregator ISymbolAggregator
	switch kind {
	case "volatility":
		aggregator = newVolatilityAggregator(symbol, period)
		point = s.GetLatestPoint(symbol, kind, period)
		point.Period = ""
	default:
		aggregator = newSymbolAggregator(symbol, period)
		point = s.GetLatestPoint(symbol, kind, period)
		point.Period = period
		aggregator.SetFirstPoint(&point)
	}

	// TODO：聚合器初始化，访问数据库构造历史数据
	aggregator.AddDefaultIndicators()
	s.aggregators[key] = aggregator
	s.subRefCounts[key] = 1

	log.Printf("[pubsub] created aggregator for %s/%s (points_per_agg=%d, indicators=%d, refCount=1)\n",
		symbol, period, aggregator.PointsPerAgg(), len(aggregator.Indicators()))
	return point
}

// Unsubscribe decrements the reference counter for the given (symbol, period).
// When the counter drops to zero or below, the aggregator and indicators are
// removed and cleanup is performed.
func (s *PubSubService) Unsubscribe(symbol, kind, period string) {
	key := fmt.Sprintf("%s_%s_%s", symbol, kind, period)

	s.mu.Lock()
	defer s.mu.Unlock()

	count, ok := s.subRefCounts[key]
	if !ok {
		log.Printf("[pubsub] unsubscribe ignored for %s/%s: no subscription record\n", symbol, period)
		return
	}

	count--
	if count > 0 {
		s.subRefCounts[key] = count
		log.Printf("[pubsub] unsubscribe ref-- for %s/%s (refCount=%d)\n",
			symbol, period, count)
		return
	}

	// Last subscriber: clean up the aggregator and its indicators
	delete(s.aggregators, key)
	delete(s.subRefCounts, key)
	log.Printf("[pubsub] removed aggregator for %s/%s (no more subscribers)\n",
		symbol, period)
}

// TODO: 调用时机要改变
// 1. 从BinanceFutureKline和AggBinanceFutureKline数据库中查询最新记录
// 2. 将查询结果转换为AggregatedKline格式，通过updateLatestPoint方法直接更新到内容
// 3. BinanceSpotKline数据记录通过GROUP BY (symbol, period)、获得symbol和period的分组，最新的一条记录
// 4. AggBinanceSpotKline数据记录通过GROUP BY (symbol, volatility)获得symbol和volatility的分组，最新的一条记录
func (s *PubSubService) loadHistoricalData() {
	if s.storage == nil {
		log.Println("[pubsub] no storage set, skipping historical data load")
		return
	}

	db := s.storage.GetDB()
	if db == nil {
		log.Println("[pubsub] no db connection, skipping historical data load")
		return
	}

	// 1. Query BinanceFutureKline - latest record per (symbol, period)
	var klines []model.BinanceFutureKline
	err := db.Raw(`
		SELECT a.*, 'kline' AS kind FROM binance_futures_kline a
		INNER JOIN (
			SELECT symbol, period, MAX(start_time) AS max_start_time
			FROM binance_futures_kline
			GROUP BY symbol, period
		) b ON a.symbol = b.symbol AND a.period = b.period AND a.start_time = b.max_start_time
	`).Scan(&klines).Error
	if err != nil {
		log.Printf("[pubsub] failed to load latest BinanceFutureKline: %v\n", err)
	} else {
		for _, k := range klines {
			point := &model.AggregatedFutureKline{
				Symbol:                   k.Symbol,
				Period:                   k.Period,
				StartTime:                k.StartTime,
				Open:                     k.Open,
				High:                     k.High,
				Low:                      k.Low,
				Close:                    k.Close,
				Volume:                   k.Volume,
				QuoteAssetVolume:         k.QuoteAssetVolume,
				Trades:                   k.Trades,
				CloseTime:                k.CloseTime,
				TakerBuyBaseAssetVolume:  k.TakerBuyBaseAssetVolume,
				TakerBuyQuoteAssetVolume: k.TakerBuyQuoteAssetVolume,
			}
			s.updateLatestPoint(point)
			log.Printf("[pubsub] loaded latest kline: %s/%s start=%d\n", k.Symbol, k.Period, k.StartTime)
		}
	}

	// 2. Query AggBinanceFutureKline - latest record per (symbol, volatility)
	var aggKlines []model.AggBinanceFutureKline
	err = db.Raw(`
		SELECT a.*, 'kline' AS kind FROM agg_binance_futures_kline a
		INNER JOIN (
			SELECT symbol, MAX(start_time) AS max_start_time
			FROM agg_binance_futures_kline
			GROUP BY symbol
		) b ON a.symbol = b.symbol AND a.start_time = b.max_start_time
	`).Scan(&aggKlines).Error
	if err != nil {
		log.Printf("[pubsub] failed to load latest AggBinanceFutureKline: %v\n", err)
	} else {
		for _, k := range aggKlines {
			point := &model.AggregatedFutureKline{
				Symbol:                   k.Symbol,
				Period:                   k.Period,
				Kind:                     "volatility",
				StartTime:                k.StartTime,
				Open:                     k.Open,
				High:                     k.High,
				Low:                      k.Low,
				Close:                    k.Close,
				Volume:                   k.Volume,
				QuoteAssetVolume:         k.QuoteAssetVolume,
				Trades:                   k.Trades,
				CloseTime:                k.CloseTime,
				TakerBuyBaseAssetVolume:  k.TakerBuyBaseAssetVolume,
				TakerBuyQuoteAssetVolume: k.TakerBuyQuoteAssetVolume,
				Count:                    k.Count,
				History:                  make([]float64, 0),
			}
			// 计算指标
			window, err := QueryVolatilityWindow(db, k.Symbol)

			if err != nil {
				log.Printf("[pubsub] failed to query volatility: %v\n", err)
			}
			wlen := len(window)
			if wlen == 0 {
				continue
			}
			sum := 0.0
			var vd float64
			for _, item := range window {
				vd = item.Volume / float64(item.Count)
				point.History = append(point.History, vd)
				sum += vd
			}
			point.Vd = vd
			ma10 := sum / float64(wlen)
			point.Ma10 = ma10
			if ma10 != 0 {
				point.Ratio = point.Vd / ma10
			} else {
				point.Ratio = 0
			}

			s.updateLatestPoint(point)
			log.Printf("[pubsub] loaded latest agg kline: %s/%s start=%d, vd=%f, ma10=%f, ratio=%f\n", k.Symbol, k.Period, k.StartTime, point.Vd, point.Ma10, point.Ratio)
		}
	}
}

// Start begins the PubSubService. It loads historical data into cache from the
// database, then connects to MQTT and starts consuming points from PointChan.
func (s *PubSubService) Start() {
	var count int
	for range s.AlignEventC {
		count++
		if count == s.eventCount {
			break // 退出 for 循环
		}
	}
	log.Printf("[pubsub] aligned %d events\n", count)
	s.loadHistoricalData()
	close(s.AlignEventC)
	close(s.LoadHistoryEventC)
	// 启动网络
	if s.conn == nil {
		panic("没有网络对象，请初始化网络")
	}
	go s.conn.run()
	s.start()
}

// start connects to MQTT and enters the main event loop.
func (s *PubSubService) start() {
	log.Println("[pubsub] started, waiting for kline points...")

	for point := range s.PointChan {
		// Update the cache
		s.updateLatestPoint(point)

		// Route the 1m point to all aggregators for this symbol
		aggs := s.addPoint(point)
		for _, agg := range aggs {
			s.publishAggregated(agg)
		}
	}
}

// updateLatestPoint caches the latest 1m kline point per symbol.
func (s *PubSubService) updateLatestPoint(point *model.AggregatedFutureKline) {
	symbol := point.Symbol
	var kind string
	if point.Kind == "volatility" {
		kind = "volatility"
	} else {
		kind = "kline"
	}

	key := fmt.Sprintf("%s_%s_%s", symbol, kind, point.Period)

	s.latestMu.Lock()
	defer s.latestMu.Unlock()
	// 指标需要保留历史数据
	old, ok := s.latestPoints[key]
	if ok && kind == "volatility" {
		point.Vd = point.Volume / float64(point.Count)
		length := len(old.History)
		if length < 10 {
			point.Ma10 = (point.Vd + old.Ma10*float64(length)) / float64(length)
			point.History = append(old.History, point.Vd)
			point.Ratio = point.Vd / point.Ma10
		} else {
			point.Ma10 = (point.Vd-old.History[0])/10.0 + old.Ma10
			point.History = append(old.History[1:], point.Vd)
			point.Ratio = point.Vd / point.Ma10
		}
	}
	s.latestPoints[key] = point
	log.Printf("[pubsub] latest points: %s\n", key)
}

// GetLatestPoint returns the latest cached 1m kline point for the given symbol.
// Returns an empty SpotKlinePoint if no data is cached.
func (s *PubSubService) GetLatestPoint(symbol, kind, period string) model.AggregatedFutureKline {
	// Note: the period parameter is currently unused — the cache stores
	// the latest 1m point per symbol regardless of period.
	// This matches the existing caller in WebSocketService which always
	// passes "1m" as the period.
	s.latestMu.RLock()
	defer s.latestMu.RUnlock()

	key := fmt.Sprintf("%s_%s_%s", symbol, kind, period)
	point, ok := s.latestPoints[key]
	if !ok || point == nil {
		log.Printf("[pubsub] no cached point for %s\n", key)
		buf, err := json.MarshalIndent(s.latestPoints, "", "  ")
		if err != nil {
			log.Printf("[pubsub] failed to marshal latest points: %v\n", err)
		} else {
			log.Printf("[pubsub] latest points: %s\n", buf)
		}
		return model.AggregatedFutureKline{}
	}
	return *point
}

// addPoint feeds a 1m point to all aggregators that match the point's symbol.
// Returns any aggregated klines that were completed by this point.
func (s *PubSubService) addPoint(point *model.AggregatedFutureKline) []*model.AggregatedFutureKline {
	s.mu.Lock()
	defer s.mu.Unlock()

	var results []*model.AggregatedFutureKline

	for key, agg := range s.aggregators {
		log.Printf("debug: aggregators: %v\n", key)
		// Only route points that match this aggregator's symbol
		if agg.Symbol() != point.Symbol {
			continue
		}
		_ = key

		complete := agg.Add(point)
		if complete != nil {
			log.Printf("[pubsub] aggregated %s/%s: %d points, start=%d -> end=%d\n",
				agg.Symbol(), agg.Period(), agg.PointsPerAgg(),
				complete.StartTime, complete.CloseTime)

			// Log indicator values
			for name, val := range complete.Indicators {
				log.Printf("[pubsub]   indicator %s: %.4f\n", name, val)
			}

			results = append(results, complete)
		}
	}

	return results
}

// publishAggregated publishes the aggregated kline to the MQTT broker.
func (s *PubSubService) publishAggregated(agg *model.AggregatedFutureKline) {
	var topic string
	switch agg.Kind {
	case "":
		topic = fmt.Sprintf("%s/%s/%s", mqttTopicPrefix, agg.Symbol, agg.Period)
	default:
		topic = fmt.Sprintf("%s/%s/%s", mqttVolatilityTopicPrefix, agg.Symbol, agg.Period)
	}

	payload, err := json.Marshal(agg)
	if err != nil {
		log.Printf("publishAggregated Marshal error: %s\n", err.Error())
		return
	}

	// Dispatch to local (in-process) subscribers first
	s.localMu.RLock()
	fmt.Printf("publish Aggregated point to %s: %s %s start=%d\n", topic, agg.Symbol, agg.Period, agg.StartTime)
	if subscribers, ok := s.localSubscribers[topic]; ok {
		for ch := range subscribers {
			select {
			case ch <- payload:
			default:
				// Channel full, skip
			}
		}
	}
	s.localMu.RUnlock()
}

// compile-time interface checks
var _ = math.Abs // keep import for potential future use
