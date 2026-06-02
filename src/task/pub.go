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
// When the close price change from the first point to the latest point
// exceeds 10%, the points are aggregated into one.
type symbolAggregator struct {
	symbol string
	period string
	points []*model.SpotKlinePoint
}

// newSymbolAggregator creates a new aggregator for the given symbol/period.
func newSymbolAggregator(symbol, period string) *symbolAggregator {
	return &symbolAggregator{
		symbol: symbol,
		period: period,
		points: make([]*model.SpotKlinePoint, 0),
	}
}

// add inserts a point into the aggregator. If the close price difference
// between the first and the latest point exceeds 10%, the aggregated kline
// is returned with true. Otherwise nil and false are returned.
func (a *symbolAggregator) add(point *model.SpotKlinePoint) (*AggregatedKline, bool) {
	a.points = append(a.points, point)
	if len(a.points) < 2 {
		return nil, false
	}

	first := a.points[0]
	last := point

	change := math.Abs(last.Close-first.Close) / first.Close
	if change > closePriceChangeThreshold {
		return a.aggregate(), true
	}

	return nil, false
}

// aggregate computes a single aggregated kline from the buffered points.
// Field aggregation rules:
//   - Open: value from the first data point
//   - Close: value from the latest data point
//   - High: max of all data points
//   - Low: min of all data points
//   - Volume, QuoteAssetVolume, Trades: sum of all data points
func (a *symbolAggregator) aggregate() *AggregatedKline {
	if len(a.points) == 0 {
		return nil
	}

	first := a.points[0]
	last := a.points[len(a.points)-1]

	agg := &AggregatedKline{
		Symbol:    a.symbol,
		Period:    a.period,
		StartTime: first.StartTime,
		Open:      first.Open,
		High:      first.High,
		Low:       first.Low,
		Close:     last.Close,
		CloseTime: last.CloseTime,
	}

	for _, p := range a.points {
		if p.High > agg.High {
			agg.High = p.High
		}
		if p.Low < agg.Low {
			agg.Low = p.Low
		}
		agg.Volume += p.Volume
		agg.QuoteAssetVolume += p.QuoteAssetVolume
		agg.Trades += p.Trades
	}

	return agg
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

		agg, err := s.addPoint(point)
		if err != nil {
			fmt.Printf("[pubsub] error aggregating point: %v\n", err)
			continue
		}

		if agg != nil {
			s.publishAggregated(agg)
			s.saveAggregated(agg)
		}
	}
}

// addPoint adds a point to the appropriate aggregator. Returns the aggregated
// kline if the group is complete (close price change > 10%), nil otherwise.
func (s *PubSubService) addPoint(point *model.SpotKlinePoint) (*AggregatedKline, error) {
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
		firstClose := agg.points[0].Close
		lastClose := agg.points[len(agg.points)-1].Close
		changePct := (math.Abs(lastClose-firstClose) / firstClose) * 100
		fmt.Printf("[pubsub] aggregated %d points: %s %s start=%d -> end=%d (close change=%.2f%%)\n",
			len(agg.points), agg.symbol, agg.period, agg.points[0].StartTime, agg.points[len(agg.points)-1].CloseTime, changePct)
		return complete, nil
	}

	return nil, nil
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
