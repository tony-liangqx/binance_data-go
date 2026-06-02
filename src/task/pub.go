package task

import (
	"fmt"
	"sync"
	"time"

	"binance.data.sync/src/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	// AggregationGroupSize is the number of 1m kline points to aggregate into one.
	AggregationGroupSize = 10

	// Default MQTT broker address
	defaultMQTTBroker = "tcp://127.0.0.1:1883"

	// MQTT topic prefix for aggregated kline data
	mqttTopicPrefix = "binance/aggregated"
)

// AggregatedKline represents a single aggregated kline point (10 * 1m -> 10m).
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
		points: make([]*model.SpotKlinePoint, 0, AggregationGroupSize),
	}
}

// add inserts a point into the aggregator. Returns the aggregated kline and true
// if the group is complete (reached AggregationGroupSize), otherwise nil and false.
func (a *symbolAggregator) add(point *model.SpotKlinePoint) (*AggregatedKline, bool) {
	a.points = append(a.points, point)
	if len(a.points) < AggregationGroupSize {
		return nil, false
	}
	return a.aggregate(), true
}

// aggregate computes a single aggregated kline from the buffered points.
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
// every AggregationGroupSize (10) points into one, and publishes the
// aggregated result to the MQTT broker (mosquitto).
type PubSubService struct {
	broker   string
	clientID string
	mqttOpts *mqtt.ClientOptions

	// pointChan receives raw kline points from Subscribers
	PointChan chan *model.SpotKlinePoint

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
		}
	}
}

// addPoint adds a point to the appropriate aggregator. Returns the aggregated
// kline if the group is complete, nil otherwise.
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
		fmt.Printf("[pubsub] aggregated %d points: %s %s start=%d -> end=%d\n",
			AggregationGroupSize, agg.symbol, agg.period, agg.points[0].StartTime, agg.points[len(agg.points)-1].CloseTime)
		return complete, nil
	}

	return nil, nil
}

// publishAggregated publishes the aggregated kline to the MQTT broker.
func (s *PubSubService) publishAggregated(agg *AggregatedKline) {
	// TODO: serialize to JSON
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
