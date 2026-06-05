package model

import (
	"errors"

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
// Uses OnConflict-style handling via Clauses to avoid duplicate key errors.
func (s *GormStorage) Commit(point *SpotKlinePoint) error {
	kline := &BinanceSpotKline{
		Symbol:           point.Symbol,
		Period:           point.Period,
		StartTime:        point.StartTime,
		DateTime:         point.DateTime,
		Open:             point.Open,
		High:             point.High,
		Low:              point.Low,
		Close:            point.Close,
		Volume:           point.Volume,
		CloseTime:        point.CloseTime,
		QuoteAssetVolume: point.QuoteAssetVolume,
		Trades:           point.Trades,
	}
	return s.db.Create(kline).Error
}

// CommitAggKline inserts an aggregated kline into the database.
func (s *GormStorage) CommitAggKline(kline *AggBinanceSpotKline) error {
	return s.db.Create(kline).Error
}

// GetLastTimeStamp retrieves the latest start_time for the given symbol and period.
// Returns 0 if no records exist.
func (s *GormStorage) GetLastTimeStamp(symbol string, period string) (int64, error) {
	var kline BinanceSpotKline
	err := s.db.Where("symbol = ? AND period = ?", symbol, period).
		Order("start_time DESC").
		First(&kline).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return kline.StartTime, nil
}
