package task

import (
	"fmt"
	"math"
	"strconv"

	"binance.data.sync/src/model"
)

// volatilityAggregator accumulates 1m kline points and produces an aggregated
// kline whenever the price change percentage exceeds 0.01 %.
//
// The trigger condition is:
//
//	changePct := (math.Abs(lastClose-firstClose) / firstClose) * 100
//	triggered when changePct > 0.01
//
// This is a price - driven aggregator, unlike the count - based symbolAggregator.
type volatilityAggregator struct {
	symbol          string
	period          string
	volatility      string
	floatVolatility float64

	// pointsPerAgg is kept for interface compliance; not used for trigger logic
	pointsPerAgg int

	// count of 1m points accumulated in the current window
	count int

	// first point of the current window — provides Open, StartTime and firstClose
	firstPoint *model.AggregatedKline

	// storage persists aggregated kline data to the database
	storage model.Storage

	// running aggregates for the current window
	high             float64
	low              float64
	volume           float64
	quoteAssetVolume float64
	trades           uint32

	// latest point values (updated on each Add)
	lastClose     float64
	lastCloseTime int64

	// previous aggregated kline ("上一个数据点")
	previousAggKline *AggregatedKline

	// indicators to calculate on each completed aggregated kline
	indicators []IIndicator
}

// newVolatilityAggregator creates a new price - change - driven aggregator for
// the given symbol / period.
func newVolatilityAggregator(symbol, volatility string, storage model.Storage) *volatilityAggregator {
	var floatVolatility float64
	value, err := strconv.Atoi(volatility)
	if err != nil {
		// 默认值
		floatVolatility = 1.0
		value = 10
	} else {
		floatVolatility = float64(value) / 10
	}
	return &volatilityAggregator{
		symbol:          symbol,
		volatility:      volatility,
		floatVolatility: floatVolatility,
		pointsPerAgg:    value,
		indicators:      make([]IIndicator, 0),
		storage:         storage,
	}
}

// Symbol returns the trading symbol this aggregator tracks.
func (a *volatilityAggregator) Symbol() string { return a.symbol }

// Period returns the aggregation period.
func (a *volatilityAggregator) Period() string { return a.period }

// PointsPerAgg returns 1 since this aggregator is not count - based.
func (a *volatilityAggregator) PointsPerAgg() int { return a.pointsPerAgg }

// SetFirstPoint sets the first point of the current aggregation window.
// Used during initialization to restore historical state.
func (a *volatilityAggregator) SetFirstPoint(point *model.AggregatedKline) {
	a.firstPoint = point
}

// FirstPoint returns the first point of the current window, or nil.
func (a *volatilityAggregator) FirstPoint() *model.AggregatedKline {
	return a.firstPoint
}

// Indicators returns the list of indicators attached to this aggregator.
func (a *volatilityAggregator) Indicators() []IIndicator {
	return a.indicators
}

// AddDefaultIndicators creates and cold - starts the default set of indicators.
func (a *volatilityAggregator) AddDefaultIndicators() {
	vd := NewVolumeDensityIndicator(a.period)
	vd.ColdStart(a.symbol, a.period)
	a.indicators = append(a.indicators, vd)
	fmt.Printf("[change_aggregator] added default indicators for %s/%s\n", a.symbol, a.period)
}

// Add inserts a 1m point into the aggregator. When the price change percentage
// exceeds 0.01 %, it produces an aggregated kline, runs all indicators, and
// returns the result. Returns nil if the threshold has not been reached.
func (a *volatilityAggregator) Add(point *model.AggregatedKline) *AggregatedKline {
	if a.firstPoint == nil || a.count == 0 {
		// First point of a new window: initialize all state
		a.firstPoint = point
		a.count = 1
	} else {
		// Accumulate point data
		a.count++
	}

	// Update running aggregates
	if a.count == 1 {
		a.high = point.High
		a.low = point.Low
		a.volume = point.Volume
		a.quoteAssetVolume = point.QuoteAssetVolume
		a.trades = point.Trades
	} else {
		if point.High > a.high {
			a.high = point.High
		}
		if point.Low < a.low {
			a.low = point.Low
		}
		a.volume += point.Volume
		a.quoteAssetVolume += point.QuoteAssetVolume
		a.trades += point.Trades
	}
	a.lastClose = point.Close
	a.lastCloseTime = point.CloseTime

	// Only check threshold when we have at least one accumulated point
	// (first point alone should not trigger)
	if a.count <= 1 {
		return nil
	}

	// Check price change percentage threshold
	firstClose := a.firstPoint.Close
	if firstClose == 0 {
		return nil
	}
	changePct := (math.Abs(a.lastClose-firstClose) / firstClose) * 100

	if changePct > a.floatVolatility {
		return a.finalize(point)
	}

	return nil
}

// finalize builds the aggregated kline, resets the window, and runs indicators.
func (a *volatilityAggregator) finalize(point *model.AggregatedKline) *AggregatedKline {
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
		Count:            a.count,
		Indicators:       make(map[string]any),
	}

	// Run all indicators on the aggregated kline
	for _, ind := range a.indicators {
		ind.Calculate(agg)
		agg.Indicators[ind.Name()] = ind.GetValue()
	}

	fmt.Printf("[change_aggregator] aggregated %s/%s: %d points, start=%d -> end=%d, changePct=%.4f%%\n",
		a.symbol, a.period, a.count, agg.StartTime, agg.CloseTime,
		(math.Abs(a.lastClose-a.firstPoint.Close)/a.firstPoint.Close)*100)

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

// compile-time interface check
var _ ISymbolAggregator = (*volatilityAggregator)(nil)
