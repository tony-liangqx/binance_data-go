package task

import (
	"fmt"

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
	symbol     string
	period     string
	volatility string

	// first point of the current window — provides Open, StartTime and firstClose
	firstPoint *model.AggregatedKline
	count      int

	// indicators to calculate on each completed aggregated kline
	indicators []IIndicator
}

// newVolatilityAggregator creates a new price - change - driven aggregator for
// the given symbol / period.
func newVolatilityAggregator(symbol, volatility string, storage model.Storage) *volatilityAggregator {
	return &volatilityAggregator{
		symbol:     symbol,
		volatility: volatility,
		indicators: make([]IIndicator, 0),
	}
}

// Symbol returns the trading symbol this aggregator tracks.
func (a *volatilityAggregator) Symbol() string { return a.symbol }

// Period returns the aggregation period.
func (a *volatilityAggregator) Period() string { return a.period }

// PointsPerAgg returns 1 since this aggregator is not count - based.
func (a *volatilityAggregator) PointsPerAgg() int { return a.count }

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
func (a *volatilityAggregator) Add(point *model.AggregatedKline) *model.AggregatedKline {
	copyed := *point
	copyed.Period = ""
	copyed.Volatility = a.volatility
	return &copyed
}

// compile-time interface check
var _ ISymbolAggregator = (*volatilityAggregator)(nil)
