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
	Add(point *model.AggregatedKline) *AggregatedKline

	// Symbol returns the trading symbol this aggregator tracks.
	Symbol() string

	// Period returns the aggregation period (e.g. "5m", "15m", "1h").
	Period() string

	// PointsPerAgg returns how many 1m points make one aggregated kline.
	PointsPerAgg() int

	// AddDefaultIndicators adds the default set of indicators and cold-starts them.
	AddDefaultIndicators()

	// SetFirstPoint sets the first point of the current aggregation window.
	// Used during initialization to restore historical state.
	SetFirstPoint(point *model.AggregatedKline)

	// FirstPoint returns the first point of the current aggregation window,
	// or nil if the window has not been initialized.
	FirstPoint() *model.AggregatedKline

	// Indicators returns the list of indicators attached to this aggregator.
	Indicators() []IIndicator
}

/*
 * 指标算法
 */
type IIndicator interface {
	Name() string
	ColdStart(symbol, period string)
	Calculate(kline *AggregatedKline)
	GetValue() float64
}
