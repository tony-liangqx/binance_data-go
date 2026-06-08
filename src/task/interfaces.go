package task

import "binance.data.sync/src/model"

type Task interface {
	Start(timeStamp int64)
}

/*
 * 聚合算法
 */
type ISymbolAggregator interface {
	// Add inserts a 1m point into the aggregator. When the aggregation window
	// is complete, it returns the aggregated kline. Returns nil if more points
	// are needed to complete the window.
	Add(point *model.AggregatedFutureKline) *model.AggregatedFutureKline

	// Symbol returns the trading symbol this aggregator tracks.
	Symbol() string

	// Period returns the aggregation period (e.g. "5m", "15m", "1h").
	Period() string

	// AddDefaultIndicators adds the default set of indicators and cold-starts them.
	AddDefaultIndicators()

	// SetFirstPoint sets the first point of the current aggregation window.
	// Used during initialization to restore historical state.
	SetFirstPoint(point *model.AggregatedFutureKline)

	// PointsPerAgg returns 1 since this aggregator is not count - based.
	PointsPerAgg() int

	// FirstPoint returns the first point of the current aggregation window,
	// or nil if the window has not been initialized.
	FirstPoint() *model.AggregatedFutureKline

	// Indicators returns the list of indicators attached to this aggregator.
	Indicators() []IIndicator
}

/*
 * 指标算法
 */
type IIndicator interface {
	Name() string
	ColdStart(symbol, period string)
	Calculate(kline *model.AggregatedFutureKline)
	GetValue() float64
}
