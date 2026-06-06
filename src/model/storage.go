package model

import "gorm.io/gorm"

type Storage interface {
	Commit(point *SpotKlinePoint) error
	CommitAggKline(kline *AggBinanceSpotKline) error
	GetLastTimeStamp(symbol string, period string) (int64, error)
	GetLastVolatilityPoint(symbol string, period string, volatility string) (*AggBinanceSpotKline, error)
	GetDB() *gorm.DB
}
