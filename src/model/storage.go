package model

import "gorm.io/gorm"

type Storage interface {
	Commit(point *FutureKlinePoint) error
	CommitAggKline(kline *AggBinanceFutureKline) error
	GetLastTimeStamp(symbol string, period string) (int64, error)
	GetLastVolatilityPoint(symbol string, period string, volatility string) (*BinanceFutureKline, error)
	GetDB() *gorm.DB
}
