package task

import (
	"fmt"
	"strconv"
	"strings"

	"binance.data.sync/src/model"
)

// symbolAggregator accumulates 1m kline points for a (symbol, period) pair
// and produces aggregated klines. It holds the previous aggregated kline
// ("上一个数据点") and an array of indicator references that are calculated
// on each new aggregated kline.
type symbolAggregator struct {
	symbol string
	period string
	kind   string

	// pointsPerAgg is how many 1m points make one aggregated kline
	// (e.g. 5 for 5m, 15 for 15m)
	pointsPerAgg int

	// count of 1m points accumulated in the current aggregation window
	count int

	// first point of the current window — provides Open, StartTime
	firstPoint *model.AggregatedFutureKline

	// running aggregates for the current window
	high             float64
	low              float64
	volume           float64
	quoteAssetVolume float64
	trades           uint32

	// latest point values (updated on each add)
	lastClose     float64
	lastCloseTime int64

	// indicators to calculate on each completed aggregated kline
	indicators []IIndicator
}

// newSymbolAggregator creates a new aggregator for the given symbol/period.
func newSymbolAggregator(symbol, period string) *symbolAggregator {
	pointsPerAgg := periodToCount(period)
	return &symbolAggregator{
		symbol:       symbol,
		period:       period,
		kind:         "kline",
		pointsPerAgg: pointsPerAgg,
		indicators:   make([]IIndicator, 0),
	}
}

// AddDefaultIndicators creates and cold-starts the default set of indicators.
func (a *symbolAggregator) AddDefaultIndicators() {
	// SMA as the default indicator
	sma := NewSMAIndicator(a.period, defaultSMAWindow)
	sma.ColdStart(a.symbol, a.period)
	a.indicators = append(a.indicators, sma)
	fmt.Printf("[aggregator] added default indicators for %s/%s\n", a.symbol, a.period)
}

// Symbol returns the trading symbol this aggregator tracks.
func (a *symbolAggregator) Symbol() string { return a.symbol }

// Period returns the aggregation period.
func (a *symbolAggregator) Period() string { return a.period }

// PointsPerAgg returns how many 1m points make one aggregated kline.
func (a *symbolAggregator) PointsPerAgg() int { return a.pointsPerAgg }

// SetFirstPoint sets the first point of the current aggregation window.
func (a *symbolAggregator) SetFirstPoint(point *model.AggregatedFutureKline) { a.firstPoint = point }

// FirstPoint returns the first point of the current window, or nil.
func (a *symbolAggregator) FirstPoint() *model.AggregatedFutureKline { return a.firstPoint }

// Indicators returns the list of indicators attached to this aggregator.
func (a *symbolAggregator) Indicators() []IIndicator { return a.indicators }

// Add inserts a 1m point into the aggregator. When the required number
// of points has been accumulated, it produces an aggregated kline, runs
// all indicators, and returns the result. Returns nil if more points are
// needed to complete the current window.
func (a *symbolAggregator) Add(point *model.AggregatedFutureKline) *model.AggregatedFutureKline {
	if point.Kind != a.kind {
		return nil
	}

	point.Period = a.period
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
func (a *symbolAggregator) finalize(point *model.AggregatedFutureKline) *model.AggregatedFutureKline {
	if a.firstPoint == nil {
		return nil
	}

	agg := &model.AggregatedFutureKline{
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
		Count:            a.count,
		Indicators:       make(map[string]any),
	}

	// Run all indicators on the aggregated kline
	// debug
	names := make([]string, 0, len(a.indicators))
	for _, ind := range a.indicators {
		names = append(names, ind.Name())
		ind.Calculate(agg)
		agg.Indicators[ind.Name()] = ind.GetValue()
	}
	fmt.Printf("debug: indicators: %v\n", names)

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
