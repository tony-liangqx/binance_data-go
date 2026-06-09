package model

import (
	"database/sql"
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
// Uses raw INSERT to avoid GORM Create incompatibilities with ClickHouse native protocol.
func (s *GormStorage) Commit(point *FutureKlinePoint) error {
	// 空指针安全判断
	if point == nil {
		return errors.New("point is nil")
	}

	// 使用 命名参数 @变量，顺序无关，不会错位
	return s.db.Exec(`
			INSERT INTO binance_futures_kline (
				symbol, period, start_time, dt,
				open, high, low, close,
				volume, quote_asset_volume, trades, close_time,
				taker_buy_base_asset_volume, taker_buy_quote_asset_volume
			) VALUES (
				@symbol, @period, @start_time, @dt,
				@open, @high, @low, @close,
				@volume, @quote_asset_volume, @trades, @close_time,
				@taker_buy_base, @taker_buy_quote
			)`,
		sql.Named("symbol", point.Symbol),
		sql.Named("period", point.Period),
		sql.Named("start_time", point.StartTime),
		sql.Named("dt", point.DateTime),
		sql.Named("open", point.Open),
		sql.Named("high", point.High),
		sql.Named("low", point.Low),
		sql.Named("close", point.Close),
		sql.Named("volume", point.Volume),
		sql.Named("quote_asset_volume", point.QuoteAssetVolume),
		sql.Named("trades", point.Trades),
		sql.Named("close_time", point.CloseTime),
		sql.Named("taker_buy_base", point.TakerBuyBaseAssetVolume),
		sql.Named("taker_buy_quote", point.TakerBuyQuoteAssetVolume),
	).Error
}

// CommitAggKline inserts an aggregated kline into the database.
func (s *GormStorage) CommitAggKline(kline *AggBinanceFutureKline) error {
	// 1. 空指针安全校验
	if kline == nil {
		return errors.New("agg kline is nil")
	}

	// 2. 使用命名参数 @xxx，彻底告别位置错误
	return s.db.Exec(`
		INSERT INTO agg_binance_futures_kline (
			symbol, period, volatility, start_time, dt,
			open, high, low, close,
			volume, quote_asset_volume, trades, close_time,
			taker_buy_base_asset_volume, taker_buy_quote_asset_volume, count
		) VALUES (
			@symbol, @period, @volatility, @start_time, @dt,
			@open, @high, @low, @close,
			@volume, @quote_asset_volume, @trades, @close_time,
			@taker_buy_base, @taker_buy_quote, @count
		)`,
		sql.Named("symbol", kline.Symbol),
		sql.Named("period", kline.Period),
		sql.Named("volatility", kline.Volatility),
		sql.Named("start_time", kline.StartTime),
		sql.Named("dt", kline.DateTime),
		sql.Named("open", kline.Open),
		sql.Named("high", kline.High),
		sql.Named("low", kline.Low),
		sql.Named("close", kline.Close),
		sql.Named("volume", kline.Volume),
		sql.Named("quote_asset_volume", kline.QuoteAssetVolume),
		sql.Named("trades", kline.Trades),
		sql.Named("close_time", kline.CloseTime),
		sql.Named("taker_buy_base", kline.TakerBuyBaseAssetVolume),
		sql.Named("taker_buy_quote", kline.TakerBuyQuoteAssetVolume),
		sql.Named("count", kline.Count),
	).Error
}

// GetLastTimeStamp retrieves the latest start_time for the given symbol and period.
// Returns 0 if no records exist.
// Uses Raw SQL to avoid GORM query builder incompatibilities with ClickHouse native protocol.
func (s *GormStorage) GetLastTimeStamp(symbol string, period string) (int64, error) {
	var startTime int64
	err := s.db.Raw(
		"SELECT start_time FROM binance_futures_kline WHERE symbol = ? AND period = ? ORDER BY start_time DESC LIMIT 1",
		symbol, period,
	).Scan(&startTime).Error
	if err != nil {
		return 0, err
	}
	return startTime, nil
}

// Returns 0 if no records exist.
// Uses Raw SQL to avoid GORM query builder incompatibilities with ClickHouse native protocol.
func (s *GormStorage) GetLastVolatilityPoint(symbol string, volatility string) (*BinanceFutureKline, error) {
	var agg_point AggBinanceFutureKline
	// 执行查询
	result := s.db.Raw(
		"SELECT * FROM agg_binance_futures_kline WHERE symbol = ? AND period = '1m' AND volatility = ? ORDER BY start_time DESC LIMIT 1",
		symbol, volatility,
	).Scan(&agg_point)

	if result.Error != nil {
		return nil, result.Error
	}

	if result.RowsAffected == 0 {
		return nil, nil
	}
	// 返回大于close_time的第一个点
	var point BinanceFutureKline
	err := s.db.Raw(
		"SELECT * FROM binance_futures_kline WHERE symbol = ? AND start_time > ? ORDER BY start_time ASC LIMIT 1",
		symbol, agg_point.CloseTime,
	).Scan(&point).Error
	if err != nil {
		return nil, err
	}
	if result.RowsAffected == 0 {
		return nil, nil
	}

	// 有数据，返回指针
	return &point, nil
}
