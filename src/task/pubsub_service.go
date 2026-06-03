package task

import (
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
	mqttTopicPrefix = "binance/aggregated"

	// closePriceChangeThreshold is the threshold (10%) for triggering aggregation
	closePriceChangeThreshold = 0.10
)

// AggregatedKline represents a single aggregated kline point.
type AggregatedKline struct {
	Symbol           string  `json:"symbol"`
	Period           string  `json:"period"`
	StartTime        int64   `json:"start_time"`
	Open             float64 `json:"open"`
	High             float64 `json:"high"`
	Low              float64 `json:"low"`
	Close            float64 `json:"close"`
	Volume           float64 `json:"volume"`
	QuoteAssetVolume float64 `json:"quote_asset_volume"`
	Trades           uint32  `json:"trades"`
	CloseTime        int64   `json:"close_time"`
}

// symbolAggregator accumulates points for a single symbol/period pair.
// Aggregation runs incrementally — only running max/min/sum values and
// the first/latest points are tracked, with no full data point cache.
type symbolAggregator struct {
	symbol string
	period string

	// count of points added so far
	count int

	// first point — provides Open, StartTime, and first.Close for trigger check
	firstPoint *model.SpotKlinePoint

	// running aggregates (max for high, min for low, sum for volume etc.)
	high             float64
	low              float64
	volume           float64
	quoteAssetVolume float64
	trades           uint32

	// latest point values, updated on each add
	lastClose     float64
	lastCloseTime int64
}

// newSymbolAggregator creates a new aggregator for the given symbol/period.
func newSymbolAggregator(symbol, period string) *symbolAggregator {
	return &symbolAggregator{
		symbol: symbol,
		period: period,
	}
}

// add inserts a point into the aggregator and updates running aggregates
// incrementally. If the close price difference between the first and the
// latest point exceeds 10%, the aggregated kline is returned with true.
// Otherwise nil and false are returned.
func (a *symbolAggregator) add(point *model.SpotKlinePoint) (*AggregatedKline, bool) {
	if a.count == 0 {
		// First point: initialize all state
		a.firstPoint = point
		a.high = point.High
		a.low = point.Low
		a.volume = point.Volume
		a.quoteAssetVolume = point.QuoteAssetVolume
		a.trades = point.Trades
		a.lastClose = point.Close
		a.lastCloseTime = point.CloseTime
		a.count = 1
		return nil, false
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

	// Check trigger: close price change > 10%
	change := math.Abs(a.lastClose-a.firstPoint.Close) / a.firstPoint.Close
	if change > closePriceChangeThreshold {
		fmt.Printf("symbol %s start %f, close %f, change: %f\n", a.symbol, a.firstPoint.Open, a.lastClose, change)
		return a.aggregate(point), true
	}
	// debug 信息
	fmt.Printf("symbol %s start %f, close %f, count: %d, change: %f\n", a.symbol, a.firstPoint.Open, a.lastClose, a.count, change)

	return nil, false
}

// aggregate computes a single aggregated kline from the running state.
func (a *symbolAggregator) aggregate(point *model.SpotKlinePoint) *AggregatedKline {
	if a.firstPoint == nil {
		return nil
	}

	retval := &AggregatedKline{
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
	}

	// reset all state
	a.firstPoint = point
	a.high = point.High
	a.low = point.Low
	a.volume = point.Volume
	a.quoteAssetVolume = point.QuoteAssetVolume
	a.trades = point.Trades
	a.lastClose = point.Close
	a.lastCloseTime = point.CloseTime
	a.count = 1

	return retval
}

// PubSubService receives SpotKlinePoint data from Subscribers, aggregates
// points when the close price change exceeds 10%, and publishes/saves the
// aggregated result.
type PubSubService struct {
	broker   string
	clientID string
	mqttOpts *mqtt.ClientOptions

	// pointChan receives raw kline points from Subscribers
	PointChan chan *model.SpotKlinePoint

	// storage persists aggregated kline data to the database
	storage model.Storage

	// aggregators maps "symbol:period" -> *symbolAggregator
	aggregators map[string]*symbolAggregator
	mu          sync.RWMutex

	// mqttClient is the MQTT client for publishing
	mqttClient mqtt.Client
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
		broker:      broker,
		clientID:    clientID,
		mqttOpts:    opts,
		PointChan:   make(chan *model.SpotKlinePoint, 1024),
		aggregators: make(map[string]*symbolAggregator),
	}
}

// SetStorage sets the storage backend for persisting aggregated kline data.
func (s *PubSubService) SetStorage(storage model.Storage) {
	s.storage = storage
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
		// Retry later in the loop
		panic(token.Error())
	} else {
		s.mqttClient = client
		fmt.Printf("[pubsub] connected to MQTT broker %s (client_id=%s)\n", s.broker, s.clientID)
		defer client.Disconnect(250)
	}

	fmt.Println("[pubsub] started, waiting for kline points...")

	for point := range s.PointChan {
		if s.mqttClient == nil || !s.mqttClient.IsConnected() {
			// Try reconnecting
			client := mqtt.NewClient(s.mqttOpts)
			if token := client.Connect(); token.Wait() && token.Error() != nil {
				fmt.Printf("[pubsub] MQTT reconnect failed: %v\n", token.Error())
				continue
			}
			s.mqttClient = client
			fmt.Printf("[pubsub] reconnected to MQTT broker %s\n", s.broker)
		}

		agg := s.addPoint(point)

		if agg != nil {
			s.publishAggregated(agg)
			s.saveAggregated(agg)
		}
	}
}

// 获取最新缓存记录
func (s *PubSubService) GetLatestPoint(symbol, period string) model.SpotKlinePoint {
	key := symbol + ":" + period

	s.mu.Lock()
	defer s.mu.Unlock()
	agg, ok := s.aggregators[key]
	if !ok {
		return model.SpotKlinePoint{}
	}
	return *agg.firstPoint
}

// addPoint adds a point to the appropriate aggregator. Returns the aggregated
// kline if the group is complete (close price change > 10%), nil otherwise.
func (s *PubSubService) addPoint(point *model.SpotKlinePoint) *AggregatedKline {
	key := point.Symbol + ":" + point.Period

	s.mu.Lock()
	defer s.mu.Unlock()

	agg, ok := s.aggregators[key]
	if !ok {
		agg = newSymbolAggregator(point.Symbol, point.Period)
		s.aggregators[key] = agg
	}

	complete, full := agg.add(point)
	if full {
		// Reset the aggregator for the next group
		s.aggregators[key] = newSymbolAggregator(point.Symbol, point.Period)
		firstClose := agg.firstPoint.Close
		lastClose := agg.lastClose
		changePct := (math.Abs(lastClose-firstClose) / firstClose) * 100
		fmt.Printf("[pubsub] aggregated %d points: %s %s start=%d -> end=%d (close change=%.2f%%)\n",
			agg.count, agg.symbol, agg.period, agg.firstPoint.StartTime, agg.lastCloseTime, changePct)
		return complete
	}

	return nil
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
