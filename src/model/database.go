package model

// BinanceSpotKline 币安现货K线数据表（ClickHouse MergeTree）
// ORDER BY (symbol, period, start_time) 优化常见查询模式：
// 1. 查询某个交易对某个周期的最新K线
// 2. 查询某个交易对某个周期的时间范围数据
type BinanceSpotKline struct {
	// 排序键（复合主键）：symbol + period + start_time
	// ClickHouse MergeTree 的 ORDER BY 即主键，用于数据排序和分区内索引
	Symbol    string `gorm:"primaryKey;column:symbol;type:String;not null;comment:交易对"`
	Period    string `gorm:"primaryKey;column:period;type:String;not null;comment:周期 1m/5m/1h/1d"`
	StartTime int64  `gorm:"primaryKey;column:start_time;type:Int64;not null;comment:K线起始时间(毫秒)"`

	DateTime int64 `gorm:"column:dt;type:DateTime64(3);not null;comment:时间(毫秒精度)"`

	// K线核心数据
	Open  float64 `gorm:"column:open;type:Float64;not null"`
	High  float64 `gorm:"column:high;type:Float64;not null"`
	Low   float64 `gorm:"column:low;type:Float64;not null"`
	Close float64 `gorm:"column:close;type:Float64;not null"`

	Volume           float64 `gorm:"column:volume;type:Float64;not null;comment:成交量"`
	QuoteAssetVolume float64 `gorm:"column:quote_asset_volume;type:Float64;not null"`
	Trades           uint32  `gorm:"column:trades;type:UInt32;not null;comment:成交笔数"`

	// 时间与状态
	CloseTime int64 `gorm:"column:close_time;type:Int64;not null"`
}

// TableName 绑定表名
func (BinanceSpotKline) TableName() string {
	return "binance_spot_kline"
}

type AggBinanceSpotKline struct {
	// 排序键（复合主键）：symbol + period + start_time
	Symbol    string `gorm:"primaryKey;column:symbol;type:String;not null"`
	Period    string `gorm:"primaryKey;column:period;type:String;not null"`
	StartTime int64  `gorm:"primaryKey;column:start_time;type:Int64;not null"`

	DateTime int64 `gorm:"column:dt;type:DateTime64(3);not null;comment:时间(毫秒精度)"`

	// K线核心数据
	Open  float64 `gorm:"column:open;type:Float64;not null"`
	High  float64 `gorm:"column:high;type:Float64;not null"`
	Low   float64 `gorm:"column:low;type:Float64;not null"`
	Close float64 `gorm:"column:close;type:Float64;not null"`

	Volume           float64 `gorm:"column:volume;type:Float64;not null;comment:成交量"`
	QuoteAssetVolume float64 `gorm:"column:quote_asset_volume;type:Float64;not null"`
	Trades           uint32  `gorm:"column:trades;type:UInt32;not null;comment:成交笔数"`

	// 时间与状态
	CloseTime int64 `gorm:"column:close_time;type:Int64;not null"`
}

// TableName 绑定表名
func (AggBinanceSpotKline) TableName() string {
	return "agg_binance_spot_kline"
}

type SpotKlinePoint struct {
	Symbol    string // VARCHAR(20) NOT NULL COMMENT '交易对',
	Period    string // VARCHAR(10) NOT NULL COMMENT '周期 1m/5m/1h/1d',
	StartTime int64  // BIGINT NOT NULL COMMENT 'K线起始时间(毫秒)',
	DateTime  int64  // DATETIME(3) NOT NULL COMMENT '时间',

	Open   float64 // DECIMAL(22,8) NOT NULL,
	High   float64 // DECIMAL(22,8) NOT NULL,
	Low    float64 // DECIMAL(22,8) NOT NULL,
	Close  float64 // DECIMAL(22,8) NOT NULL,
	Volume float64 // DECIMAL(32,8) NOT NULL,

	CloseTime        int64   // BIGINT NOT NULL,
	QuoteAssetVolume float64 // DECIMAL(32,8) NOT NULL,
	Trades           uint32  // INT UNSIGNED NOT NULL,
}
