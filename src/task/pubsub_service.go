package task

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"binance.data.sync/src/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	// Default MQTT broker address
	defaultMQTTBroker = "tcp://127.0.0.1:1883"

	// MQTT topic prefix for aggregated kline data
	mqttTopicPrefix = "binance/aggregated"

	// Default SMA window size for indicators created on subscription
	defaultSMAWindow = 14
)

// AggregatedKline represents a single aggregated kline point produced
// by aggregating multiple 1m klines over a user-specified period.
type AggregatedKline struct {
	Symbol           string         `json:"symbol"`
	Period           string         `json:"period"`
	StartTime        int64          `json:"start_time"`
	Open             float64        `json:"open"`
	High             float64        `json:"high"`
	Low              float64        `json:"low"`
	Close            float64        `json:"close"`
	Volume           float64        `json:"volume"`
	QuoteAssetVolume float64        `json:"quote_asset_volume"`
	Trades           uint32         `json:"trades"`
	CloseTime        int64          `json:"close_time"`
	Indicators       map[string]any `json:"indicators,omitempty"`
}

// symbolAggregator accumulates 1m kline points for a (symbol, period) pair
// and produces aggregated klines. It holds the previous aggregated kline
// ("上一个数据点") and an array of indicator references that are calculated
// on each new aggregated kline.
type symbolAggregator struct {
	symbol string
	period string

	// pointsPerAgg is how many 1m points make one aggregated kline
	// (e.g. 5 for 5m, 15 for 15m)
	pointsPerAgg int

	// count of 1m points accumulated in the current aggregation window
	count int

	// first point of the current window — provides Open, StartTime
	firstPoint *model.SpotKlinePoint

	// running aggregates for the current window
	high             float64
	low              float64
	volume           float64
	quoteAssetVolume float64
	trades           uint32

	// latest point values (updated on each add)
	lastClose     float64
	lastCloseTime int64

	// previous aggregated kline ("上一个数据点")
	// Used as context for the next aggregation cycle
	previousAggKline *AggregatedKline

	// indicators to calculate on each completed aggregated kline
	indicators []IIndicator
}

// newSymbolAggregator creates a new aggregator for the given symbol/period.
func newSymbolAggregator(symbol, period string) *symbolAggregator {
	pointsPerAgg := periodToCount(period)
	return &symbolAggregator{
		symbol:       symbol,
		period:       period,
		pointsPerAgg: pointsPerAgg,
		indicators:   make([]IIndicator, 0),
	}
}

// addDefaultIndicators creates and cold-starts the default set of indicators.
func (a *symbolAggregator) addDefaultIndicators() {
	// SMA as the default indicator
	sma := NewSMAIndicator(a.period, defaultSMAWindow)
	sma.ColdStart(a.symbol, a.period)
	a.indicators = append(a.indicators, sma)
	fmt.Printf("[aggregator] added default indicators for %s/%s\n", a.symbol, a.period)
}

// add inserts a 1m point into the aggregator. When the required number
// of points has been accumulated, it produces an aggregated kline, runs
// all indicators, and returns the result. Returns nil if more points are
// needed to complete the current window.
func (a *symbolAggregator) add(point *model.SpotKlinePoint) *AggregatedKline {
	if a.count == 0 {
		// First point of a new window: initialize all state
		a.firstPoint = point
		a.high = point.High
		a.low = point.Low
		a.volume = point.Volume
		a.quoteAssetVolume = point.QuoteAssetVolume
		a.trades = point.Trades
		a.lastClose = point.Close
		a.lastCloseTime = point.CloseTime
		a.count = 1

		if a.pointsPerAgg <= 1 {
			return a.finalize(point)
		}
		return nil
	}

	// Update running aggregates incrementally
	if point.High > a.high {
		a.high = point.High
	}
	if point.Low < a.low {
		a.low = point.Low
	}
	a.volume += point.Volume
	a.quoteAssetVolume += point.QuoteAssetVolume
	a.trades += point.Trades
	a.lastClose = point.Close
	a.lastCloseTime = point.CloseTime
	a.count++

	if a.count >= a.pointsPerAgg {
		return a.finalize(point)
	}

	return nil
}

// finalize builds the aggregated kline, resets the window, runs indicators.
func (a *symbolAggregator) finalize(point *model.SpotKlinePoint) *AggregatedKline {
	if a.firstPoint == nil {
		return nil
	}

	agg := &AggregatedKline{
		Symbol:           a.symbol,
		Period:           a.period,
		StartTime:        a.firstPoint.StartTime,
		Open:             a.firstPoint.Open,
		High:             a.high,
		Low:              a.low,
		Close:            a.lastClose,
		Volume:           a.volume,
		QuoteAssetVolume: a.quoteAssetVolume,
		Trades:           a.trades,
		CloseTime:        a.lastCloseTime,
		Indicators:       make(map[string]any),
	}

	// Run all indicators on the aggregated kline
	for _, ind := range a.indicators {
		ind.Calculate(agg)
		agg.Indicators[ind.Name()] = ind.GetValue()
	}

	// Store as the "previous data point" for context in the next cycle
	a.previousAggKline = agg

	// Reset window state, using the current point as the start of the next window
	a.firstPoint = point
	a.high = point.High
	a.low = point.Low
	a.volume = point.Volume
	a.quoteAssetVolume = point.QuoteAssetVolume
	a.trades = point.Trades
	a.lastClose = point.Close
	a.lastCloseTime = point.CloseTime
	a.count = 1

	return agg
}

// periodToCount converts a period string to the number of 1m klines needed.
// Supported formats: 1m, 5m, 15m, 30m, 1h, 4h, 1d
func periodToCount(period string) int {
	period = strings.ToLower(period)

	if strings.HasSuffix(period, "m") {
		n, err := strconv.Atoi(strings.TrimSuffix(period, "m"))
		if err != nil || n <= 0 {
			return 1
		}
		return n
	}

	if strings.HasSuffix(period, "h") {
		n, err := strconv.Atoi(strings.TrimSuffix(period, "h"))
		if err != nil || n <= 0 {
			return 60
		}
		return n * 60
	}

	if strings.HasSuffix(period, "d") {
		n, err := strconv.Atoi(strings.TrimSuffix(period, "d"))
		if err != nil || n <= 0 {
			return 1440
		}
		return n * 1440
	}

	// Default: assume it's already in minutes
	n, err := strconv.Atoi(period)
	if err != nil || n <= 0 {
		return 1
	}
	return n
}

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
	PointChan chan *model.SpotKlinePoint

	// storage persists aggregated kline data to the database
	storage model.Storage

	// aggregators maps "symbol:period" -> *symbolAggregator
	// Created on-demand when users subscribe to aggregated streams
	aggregators map[string]*symbolAggregator
	mu          sync.RWMutex

	// mqttClient is the MQTT client for publishing
	mqttClient mqtt.Client

	// latestPoints caches the latest 1m point per symbol (key = symbol)
	latestPoints map[string]*model.SpotKlinePoint
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
		PointChan:    make(chan *model.SpotKlinePoint, 1024),
		aggregators:  make(map[string]*symbolAggregator),
		latestPoints: make(map[string]*model.SpotKlinePoint),
	}
}

// SetStorage sets the storage backend for persisting aggregated kline data.
func (s *PubSubService) SetStorage(storage model.Storage) {
	s.storage = storage
}

// Subscribe creates a symbolAggregator for the given (symbol, period) pair
// with default indicators. This is called when a user subscribes to an
// aggregated stream (e.g. "binance/aggregated/btcusdt/5m").
//
// If an aggregator already exists for this (symbol, period), it is returned
// without re-creating.
func (s *PubSubService) Subscribe(symbol, period string) {
	key := symbol + ":" + period

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.aggregators[key]; ok {
		fmt.Printf("[pubsub] aggregator already exists for %s/%s\n", symbol, period)
		return
	}

	agg := newSymbolAggregator(symbol, period)
	agg.addDefaultIndicators()
	s.aggregators[key] = agg

	fmt.Printf("[pubsub] created aggregator for %s/%s (points_per_agg=%d, indicators=%d)\n",
		symbol, period, agg.pointsPerAgg, len(agg.indicators))
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

		// Update the 1m cache for this symbol
		s.updateLatestPoint(point)

		// Route the 1m point to all aggregators for this symbol
		aggs := s.addPoint(point)
		for _, agg := range aggs {
			s.publishAggregated(agg)
			// s.saveAggregated(agg)
		}
	}
}

// updateLatestPoint caches the latest 1m kline point per symbol.
func (s *PubSubService) updateLatestPoint(point *model.SpotKlinePoint) {
	s.latestMu.Lock()
	defer s.latestMu.Unlock()

	existing, ok := s.latestPoints[point.Symbol]
	if !ok || point.StartTime > existing.StartTime {
		s.latestPoints[point.Symbol] = point
	}
}

// GetLatestPoint returns the latest cached 1m kline point for the given symbol.
// Returns an empty SpotKlinePoint if no data is cached.
func (s *PubSubService) GetLatestPoint(symbol, period string) model.SpotKlinePoint {
	// Note: the period parameter is currently unused — the cache stores
	// the latest 1m point per symbol regardless of period.
	// This matches the existing caller in WebSocketService which always
	// passes "1m" as the period.
	s.latestMu.RLock()
	defer s.latestMu.RUnlock()

	point, ok := s.latestPoints[symbol]
	if !ok || point == nil {
		return model.SpotKlinePoint{}
	}
	return *point
}

// addPoint feeds a 1m point to all aggregators that match the point's symbol.
// Returns any aggregated klines that were completed by this point.
func (s *PubSubService) addPoint(point *model.SpotKlinePoint) []*AggregatedKline {
	s.mu.Lock()
	defer s.mu.Unlock()

	var results []*AggregatedKline

	for key, agg := range s.aggregators {
		// Only route points that match this aggregator's symbol
		if agg.symbol != point.Symbol {
			continue
		}
		_ = key

		complete := agg.add(point)
		if complete != nil {
			fmt.Printf("[pubsub] aggregated %s/%s: %d points, start=%d -> end=%d\n",
				agg.symbol, agg.period, agg.pointsPerAgg,
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
func (s *PubSubService) publishAggregated(agg *AggregatedKline) {
	topic := fmt.Sprintf("%s/%s/%s", mqttTopicPrefix, agg.Symbol, agg.Period)
	payload := fmt.Sprintf(
		`{"symbol":"%s","period":"%s","start_time":%d,"open":%f,"high":%f,"low":%f,"close":%f,"volume":%f,"quote_asset_volume":%f,"trades":%d,"close_time":%d}`,
		agg.Symbol, agg.Period, agg.StartTime,
		agg.Open, agg.High, agg.Low, agg.Close,
		agg.Volume, agg.QuoteAssetVolume, agg.Trades, agg.CloseTime,
	)

	token := s.mqttClient.Publish(topic, 1, false, payload)
	token.Wait()
	if token.Error() != nil {
		fmt.Printf("[pubsub] failed to publish aggregated kline: %v\n", token.Error())
	} else {
		fmt.Printf("[pubsub] published aggregated kline: %s %s start=%d\n",
			agg.Symbol, agg.Period, agg.StartTime)
	}
}

// saveAggregated writes the aggregated kline to the AggBinanceSpotKline table.
func (s *PubSubService) saveAggregated(agg *AggregatedKline) {
	if s.storage == nil {
		return
	}

	kline := &model.AggBinanceSpotKline{
		Symbol:           agg.Symbol,
		Period:           agg.Period,
		StartTime:        agg.StartTime,
		DateTime:         time.UnixMilli(agg.StartTime),
		Open:             agg.Open,
		High:             agg.High,
		Low:              agg.Low,
		Close:            agg.Close,
		Volume:           agg.Volume,
		CloseTime:        agg.CloseTime,
		QuoteAssetVolume: agg.QuoteAssetVolume,
		Trades:           agg.Trades,
	}

	if err := s.storage.CommitAggKline(kline); err != nil {
		fmt.Printf("[pubsub] failed to save aggregated kline: %v\n", err)
	} else {
		fmt.Printf("[pubsub] saved aggregated kline: %s %s start=%d\n",
			agg.Symbol, agg.Period, agg.StartTime)
	}
}

// compile-time interface checks
var _ = math.Abs // keep import for potential future use
