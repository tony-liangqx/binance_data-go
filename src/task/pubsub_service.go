package task

import (
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"binance.data.sync/src/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
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
	mqttOpts *mqtt.ClientOptions

	// PointChan receives raw 1m kline points from Subscribers
	PointChan chan *model.AggregatedKline

	// storage persists aggregated kline data to the database
	storage model.Storage

	// aggregators maps "symbol:period" -> ISymbolAggregator
	// Created on-demand when users subscribe to aggregated streams
	aggregators  map[string]ISymbolAggregator
	subRefCounts map[string]int // reference count per "symbol:period"
	mu           sync.RWMutex

	// mqttClient is the MQTT client for publishing
	mqttClient mqtt.Client

	// latestPoints caches the latest 1m point per symbol (key = symbol)
	latestPoints map[string]*model.AggregatedKline
	latestMu     sync.RWMutex
}

// NewPubSubService creates a new PubSubService with default MQTT broker settings.
func NewPubSubService() *PubSubService {
	return NewPubSubServiceWithBroker(defaultMQTTBroker)
}

// NewPubSubServiceWithBroker creates a new PubSubService with a custom MQTT broker address.
func NewPubSubServiceWithBroker(broker string) *PubSubService {
	clientID := fmt.Sprintf("binance_pubsub_%d", time.Now().UnixNano())

	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(clientID)
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(true)
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetOrderMatters(false)

	return &PubSubService{
		broker:       broker,
		clientID:     clientID,
		mqttOpts:     opts,
		PointChan:    make(chan *model.AggregatedKline, 1024),
		aggregators:  make(map[string]ISymbolAggregator),
		subRefCounts: make(map[string]int),
		latestPoints: make(map[string]*model.AggregatedKline),
	}
}

// SetStorage sets the storage backend for persisting aggregated kline data.
func (s *PubSubService) SetStorage(storage model.Storage) {
	s.storage = storage
}

// Subscribe creates a symbolAggregator for the given (symbol, period) pair
// with default indicators. This is called when a user subscribes to an
// aggregated stream (e.g. "BTCUSDT@kline_1m/ETHUSDT@kline_1m").
//
// It maintains a reference counter for each (symbol, period). The aggregator
// is only created on the first subscription and removed when the last
// subscriber calls Unsubscribe.
func (s *PubSubService) Subscribe(symbol, kind, period string) model.AggregatedKline {
	key := fmt.Sprintf("%s_%s_%s", symbol, kind, period)

	s.mu.Lock()
	defer s.mu.Unlock()

	if agg, ok := s.aggregators[key]; ok {
		s.subRefCounts[key]++
		fmt.Printf("[pubsub] subscribe ref++ for %s/%s (refCount=%d, indicators=%d)\n",
			symbol, period, s.subRefCounts[key], len(agg.Indicators()))
		// TODO：聚合器初始化，访问数据库构造历史数据
		return *(agg.FirstPoint())
	}

	var agg ISymbolAggregator
	var point model.AggregatedKline
	switch kind {
	case "volatility":
		agg = newVolatilityAggregator(symbol, period)
		// 返回不同类型的数据
		point = s.GetLatestPoint(symbol, kind, "1m")
		point = *(agg.Add(&point))
	default:
		agg = newSymbolAggregator(symbol, period)
		point = s.GetLatestPoint(symbol, kind, "1m")
		point.Period = period
		agg.SetFirstPoint(&point)
	}

	// TODO：聚合器初始化，访问数据库构造历史数据
	agg.AddDefaultIndicators()
	s.aggregators[key] = agg
	s.subRefCounts[key] = 1

	fmt.Printf("[pubsub] created aggregator for %s/%s (points_per_agg=%d, indicators=%d, refCount=1)\n",
		symbol, period, agg.PointsPerAgg(), len(agg.Indicators()))
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
		fmt.Printf("[pubsub] unsubscribe ignored for %s/%s: no subscription record\n", symbol, period)
		return
	}

	count--
	if count > 0 {
		s.subRefCounts[key] = count
		fmt.Printf("[pubsub] unsubscribe ref-- for %s/%s (refCount=%d)\n",
			symbol, period, count)
		return
	}

	// Last subscriber: clean up the aggregator and its indicators
	delete(s.aggregators, key)
	delete(s.subRefCounts, key)
	fmt.Printf("[pubsub] removed aggregator for %s/%s (no more subscribers)\n",
		symbol, period)
}

// Start begins the PubSubService. It connects to MQTT and starts consuming
// points from PointChan.
func (s *PubSubService) Start() {
	s.start()
}

// start connects to MQTT and enters the main event loop.
func (s *PubSubService) start() {
	// Connect to MQTT broker
	client := mqtt.NewClient(s.mqttOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Printf("[pubsub] failed to connect to MQTT broker %s: %v\n", s.broker, token.Error())
		panic(token.Error())
	} else {
		s.mqttClient = client
		fmt.Printf("[pubsub] connected to MQTT broker %s (client_id=%s)\n", s.broker, s.clientID)
		defer client.Disconnect(250)
	}

	fmt.Println("[pubsub] started, waiting for kline points...")

	for point := range s.PointChan {
		if s.mqttClient == nil || !s.mqttClient.IsConnected() {
			client := mqtt.NewClient(s.mqttOpts)
			if token := client.Connect(); token.Wait() && token.Error() != nil {
				fmt.Printf("[pubsub] MQTT reconnect failed: %v\n", token.Error())
				continue
			}
			s.mqttClient = client
			fmt.Printf("[pubsub] reconnected to MQTT broker %s\n", s.broker)
		}

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
func (s *PubSubService) updateLatestPoint(point *model.AggregatedKline) {
	symbol := point.Symbol
	period := point.Period
	var kind string
	if point.Volatility != "" {
		kind = point.Volatility
	} else {
		kind = "kline"
	}

	key := fmt.Sprintf("%s_%s_%s", symbol, kind, period)

	s.latestMu.Lock()
	defer s.latestMu.Unlock()
	s.latestPoints[key] = point
	fmt.Printf("[pubsub] latest points: %s\n", key)
}

// GetLatestPoint returns the latest cached 1m kline point for the given symbol.
// Returns an empty SpotKlinePoint if no data is cached.
func (s *PubSubService) GetLatestPoint(symbol, kind, period string) model.AggregatedKline {
	// Note: the period parameter is currently unused — the cache stores
	// the latest 1m point per symbol regardless of period.
	// This matches the existing caller in WebSocketService which always
	// passes "1m" as the period.
	s.latestMu.RLock()
	defer s.latestMu.RUnlock()

	key := fmt.Sprintf("%s_%s_%s", symbol, kind, period)
	point, ok := s.latestPoints[key]
	if !ok || point == nil {
		buf, err := json.MarshalIndent(s.latestPoints, "", "  ")
		if err != nil {
			fmt.Printf("[pubsub] failed to marshal latest points: %v\n", err)
		} else {
			fmt.Printf("[pubsub] latest points: %s\n", buf)
		}
		return model.AggregatedKline{}
	}
	return *point
}

// addPoint feeds a 1m point to all aggregators that match the point's symbol.
// Returns any aggregated klines that were completed by this point.
func (s *PubSubService) addPoint(point *model.AggregatedKline) []*model.AggregatedKline {
	s.mu.Lock()
	defer s.mu.Unlock()

	var results []*model.AggregatedKline

	for key, agg := range s.aggregators {
		fmt.Printf("debug: aggregators: %v\n", key)
		// Only route points that match this aggregator's symbol
		if agg.Symbol() != point.Symbol {
			continue
		}
		_ = key

		complete := agg.Add(point)
		if complete != nil {
			fmt.Printf("[pubsub] aggregated %s/%s: %d points, start=%d -> end=%d\n",
				agg.Symbol(), agg.Period(), agg.PointsPerAgg(),
				complete.StartTime, complete.CloseTime)

			// Log indicator values
			for name, val := range complete.Indicators {
				fmt.Printf("[pubsub]   indicator %s: %.4f\n", name, val)
			}

			results = append(results, complete)
		}
	}

	return results
}

// publishAggregated publishes the aggregated kline to the MQTT broker.
func (s *PubSubService) publishAggregated(agg *model.AggregatedKline) {
	var topic string
	switch agg.Volatility {
	case "":
		topic = fmt.Sprintf("%s/%s/%s", mqttTopicPrefix, agg.Symbol, agg.Period)
	default:
		topic = fmt.Sprintf("%s/%s/%s", mqttVolatilityTopicPrefix, agg.Symbol, agg.Volatility)
	}

	payload, err := json.Marshal(agg)
	if err != nil {
		fmt.Printf("publishAggregated Marshal error: %s\n", err.Error())
		return
	}

	token := s.mqttClient.Publish(topic, 1, false, payload)
	token.Wait()
	if token.Error() != nil {
		fmt.Printf("[pubsub] failed to publish aggregated kline: %v\n", token.Error())
	} else {
		fmt.Printf("[pubsub] published aggregated kline: %s %s start=%d\n",
			agg.Symbol, agg.Period, agg.StartTime)
	}
}

// compile-time interface checks
var _ = math.Abs // keep import for potential future use
