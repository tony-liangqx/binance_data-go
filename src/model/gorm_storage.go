package model

import (
	"gorm.io/gorm"
)

// GormStorage implements Storage interface using GORM
type GormStorage struct {
	db *gorm.DB
}

// NewGormStorage creates a new GormStorage instance
func NewGormStorage(db *gorm.DB) *GormStorage {
	return &GormStorage{db: db}
}

func (s *GormStorage) GetDB() *gorm.DB {
	return s.db
}

// Commit inserts a kline point into the database.
// Uses raw INSERT to avoid GORM Create incompatibilities with ClickHouse native protocol.
func (s *GormStorage) Commit(point *SpotKlinePoint) error {
	return s.db.Exec(
		`INSERT INTO binance_spot_kline (symbol, period, start_time, dt, open, high, low, close, volume, quote_asset_volume, trades, close_time)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		point.Symbol, point.Period, point.StartTime, point.DateTime,
		point.Open, point.High, point.Low, point.Close,
		point.Volume, point.QuoteAssetVolume, point.Trades, point.CloseTime,
	).Error
}

// CommitAggKline inserts an aggregated kline into the database.
func (s *GormStorage) CommitAggKline(kline *AggBinanceSpotKline) error {
	return s.db.Exec(
		`INSERT INTO agg_binance_spot_kline (symbol, period, volatility, start_time, dt, open, high, low, close, volume, quote_asset_volume, trades, close_time)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		kline.Symbol, kline.Period, kline.Volatility, kline.StartTime, kline.DateTime,
		kline.Open, kline.High, kline.Low, kline.Close,
		kline.Volume, kline.QuoteAssetVolume, kline.Trades, kline.CloseTime,
	).Error
}

// GetLastTimeStamp retrieves the latest start_time for the given symbol and period.
// Returns 0 if no records exist.
// Uses Raw SQL to avoid GORM query builder incompatibilities with ClickHouse native protocol.
func (s *GormStorage) GetLastTimeStamp(symbol string, period string) (int64, error) {
	var startTime int64
	err := s.db.Raw(
		"SELECT start_time FROM binance_spot_kline WHERE symbol = ? AND period = ? ORDER BY start_time DESC LIMIT 1",
		symbol, period,
	).Scan(&startTime).Error
	if err != nil {
		return 0, err
	}
	return startTime, nil
}

// Returns 0 if no records exist.
// Uses Raw SQL to avoid GORM query builder incompatibilities with ClickHouse native protocol.
func (s *GormStorage) GetLastVolatilityPoint(symbol string, period string, volatility string) (*AggBinanceSpotKline, error) {
	var point AggBinanceSpotKline
	err := s.db.Raw(
		"SELECT * FROM agg_binance_spot_kline WHERE symbol = ? AND period = ? AND volatility = ? ORDER BY start_time DESC LIMIT 1",
		symbol, period, volatility,
	).Scan(&point).Error
	if err != nil {
		return nil, err
	}
	return &point, nil
}
