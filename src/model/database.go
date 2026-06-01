package model

import (
	"time"
)

// BinanceSpotKline 币安现货K线数据表
type BinanceSpotKline struct {
	// 自增主键
	ID uint64 `gorm:"column:id;type:bigint unsigned;not null;autoIncrement;primaryKey"`

	// 联合唯一索引：symbol + period + start_time
	Symbol    string `gorm:"column:symbol;type:varchar(20);not null;index:idx_symbol_period_time,unique"`
	Period    string `gorm:"column:period;type:varchar(10);not null;index:idx_symbol_period_time,unique"`
	StartTime int64  `gorm:"column:start_time;type:bigint;not null;index:idx_symbol_period_time,unique"`

	DateTime time.Time `gorm:"column:dt;type:datetime(3);not null;comment:时间"`

	// K线核心数据
	Open  float64 `gorm:"column:open;type:decimal(22,8);not null"`
	High  float64 `gorm:"column:high;type:decimal(22,8);not null"`
	Low   float64 `gorm:"column:low;type:decimal(22,8);not null"`
	Close float64 `gorm:"column:close;type:decimal(22,8);not null"`

	Volume           float64 `gorm:"column:volume;type:decimal(32,8);not null;comment:成交量"`
	QuoteAssetVolume float64 `gorm:"column:quote_asset_volume;type:decimal(32,8);not null"`
	Trades           uint32  `gorm:"column:trades;type:int unsigned;not null;comment:成交笔数"`

	// 时间与状态
	CloseTime int64 `gorm:"column:close_time;type:bigint;not null"`
}

// TableName 绑定表名
func (BinanceSpotKline) TableName() string {
	return "binance_spot_kline"
}

type SpotKlinePoint struct {
	Symbol    string //` VARCHAR(20) NOT NULL COMMENT '交易对',
	Period    string // VARCHAR(10) NOT NULL COMMENT '周期 1m/5m/1h/1d',
	StartTime int64  // BIGINT NOT NULL COMMENT 'K线起始时间(毫秒)',
	DateTime  int64  //` DATETIME(3) NOT NULL COMMENT '时间',

	Open   float64 // DECIMAL(22,8) NOT NULL,
	High   float64 // DECIMAL(22,8) NOT NULL,
	Low    float64 // DECIMAL(22,8) NOT NULL,
	Close  float64 // DECIMAL(22,8) NOT NULL,
	Volume float64 // DECIMAL(32,8) NOT NULL,

	CloseTime        int64   // BIGINT NOT NULL,
	QuoteAssetVolume float64 // DECIMAL(32,8) NOT NULL,
	Trades           uint32  // INT UNSIGNED NOT NULL,
}
