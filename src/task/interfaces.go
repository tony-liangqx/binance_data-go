package task

import "binance.data.sync/src/model"

type Task interface {
	Start(timeStamp int64)
}

/*
 * 聚合算法
 */
type ISymbolAggregator interface {
	Add(point *model.SpotKlinePoint) (*AggregatedKline, bool)
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
