package task

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"binance.data.sync/src/model"
	"gorm.io/gorm"
)

// volatilityHandler handles GET /fapi/v1/volatility requests.
// It queries historical aggregated volatility kline data from the
// agg_binance_futures_kline table.
type volatilityHandler struct {
	storage model.Storage
}

// volatilityRecord represents a single volatility kline in the JSON response.
type volatilityRecord struct {
	Symbol                   string  `json:"symbol"`
	Volatility               string  `json:"volatility"`
	StartTime                int64   `json:"start_time"`
	Open                     float64 `json:"open"`
	High                     float64 `json:"high"`
	Low                      float64 `json:"low"`
	Close                    float64 `json:"close"`
	Volume                   float64 `json:"volume"`
	QuoteAssetVolume         float64 `json:"quote_asset_volume"`
	Trades                   uint32  `json:"trades"`
	TakerBuyBaseAssetVolume  float64 `json:"taker_buy_base_asset_volume"`
	TakerBuyQuoteAssetVolume float64 `json:"taker_buy_quote_asset_volume"`
	CloseTime                int64   `json:"close_time"`
	Count                    int     `json:"count"`
	Vd                       float64 `gorm:"-" json:"vd"`
	Ma10                     float64 `gorm:"-" json:"ma10"`
	Ratio                    float64 `gorm:"-" json:"ratio"`
}

type AggKlineWithIndicator struct {
	// 排序键（复合主键）：symbol + period + start_time
	Symbol     string `gorm:"column:symbol"`
	Volatility string `gorm:"column:volatility"`
	StartTime  int64  `gorm:"column:start_time"`
	// K线核心数据
	Open                     float64 `gorm:"column:open"`
	High                     float64 `gorm:"column:high"`
	Low                      float64 `gorm:"column:low"`
	Close                    float64 `gorm:"column:close"`
	Volume                   float64 `gorm:"column:volume"`
	QuoteAssetVolume         float64 `gorm:"column:quote_asset_volume"`
	Trades                   uint32  `gorm:"column:trades"`
	TakerBuyBaseAssetVolume  float64 `gorm:"column:taker_buy_base_asset_volume"`
	TakerBuyQuoteAssetVolume float64 `gorm:"column:taker_buy_quote_asset_volume"`
	// 时间与状态
	CloseTime int64   `gorm:"column:close_time"`
	Count     int     `gorm:"column:count"`
	Vd        float64 `gorm:"column:vd"`
	Ma10      float64 `gorm:"column:ma10"`
	Ratio     float64 `gorm:"column:ratio"`
}

// newVolatilityRecord builds a volatilityRecord from a model row.
func newVolatilityRecord(k *AggKlineWithIndicator) volatilityRecord {
	return volatilityRecord{
		Symbol: k.Symbol,
		// Period:               k.Period,
		Volatility:               k.Volatility,
		StartTime:                k.StartTime,
		Open:                     k.Open,
		High:                     k.High,
		Low:                      k.Low,
		Close:                    k.Close,
		Volume:                   k.Volume,
		QuoteAssetVolume:         k.QuoteAssetVolume,
		Trades:                   k.Trades,
		TakerBuyBaseAssetVolume:  k.TakerBuyBaseAssetVolume,
		TakerBuyQuoteAssetVolume: k.TakerBuyQuoteAssetVolume,
		CloseTime:                k.CloseTime,
		Count:                    k.Count,
		Vd:                       k.Vd,
		Ma10:                     k.Ma10,
		Ratio:                    k.Ratio,
	}
}

// isValidVolatilityInterval checks if the interval is one of the supported volatility values.
func isValidVolatilityInterval(interval string) bool {
	switch interval {
	case "5", "10", "20":
		return true
	}
	return false
}

// ServeHTTP implements the http.Handler interface.
func (h *volatilityHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, -1102, "Method not allowed.")
		return
	}

	// Parse required parameters
	symbol := r.URL.Query().Get("symbol")
	interval := r.URL.Query().Get("interval")

	if symbol == "" {
		writeAPIError(w, -1102, "Mandatory parameter 'symbol' was not sent, was empty/null, or malformed.")
		return
	}
	if interval == "" {
		writeAPIError(w, -1102, "Mandatory parameter 'interval' was not sent, was empty/null, or malformed.")
		return
	}

	// Normalize symbol to uppercase (database stores uppercase)
	symbol = strings.ToUpper(symbol)

	// Validate interval (must be one of the supported volatility values)
	if !isValidVolatilityInterval(interval) {
		writeAPIError(w, -1100, fmt.Sprintf("Invalid interval: %s", interval))
		return
	}

	// Parse optional parameters
	limit := 500
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil || parsed <= 0 {
			writeAPIError(w, -1103, "Invalid 'limit' parameter.")
			return
		}
		if parsed > 1500 {
			limit = 1500
		} else {
			limit = parsed
		}
	}

	// Parse optional time range
	var startTime, endTime int64
	hasStartTime := false
	hasEndTime := false

	if startStr := r.URL.Query().Get("startTime"); startStr != "" {
		parsed, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			writeAPIError(w, -1103, "Invalid 'startTime' parameter.")
			return
		}
		startTime = parsed
		hasStartTime = true
	}

	if endStr := r.URL.Query().Get("endTime"); endStr != "" {
		parsed, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			writeAPIError(w, -1103, "Invalid 'endTime' parameter.")
			return
		}
		endTime = parsed
		hasEndTime = true
	}

	db := h.storage.GetDB()
	if db == nil {
		writeAPIError(w, -1001, "Internal error: database not available.")
		return
	}

	// Query agg_binance_futures_kline directly (volatility data is pre-computed)
	klines, err := h.queryVolatility(db, symbol, interval, startTime, endTime, hasStartTime, hasEndTime, limit)
	if err != nil {
		fmt.Printf("[volatility-api] query error: %v\n", err)
		writeAPIError(w, -1001, "Internal error: failed to query volatility data.")
		return
	}

	// Convert to response format
	records := make([]volatilityRecord, 0, len(klines))
	for i := range klines {
		records = append(records, newVolatilityRecord(&klines[i]))
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(records); err != nil {
		fmt.Printf("[volatility-api] response encoding error: %v\n", err)
	}
}

// queryVolatility executes a direct query against agg_binance_futures_kline
// for the given symbol and volatility interval.
func (h *volatilityHandler) queryVolatility(db *gorm.DB, symbol, interval string, startTime, endTime int64, hasStartTime, hasEndTime bool, limit int) ([]AggKlineWithIndicator, error) {
	var klines []AggKlineWithIndicator

	// 1. 获取当前周期的秒数，用于历史数据向前对齐（例如 1m = 60秒）
	// 注意：请确保能根据你的 period/interval 变量映射出每根 K 线的实际秒数。这里假设一个默认基础步长，请根据业务调整
	periodSeconds := int64(60)

	// 2. 构建内部基础查询的过滤条件与参数绑定
	baseWhere := "WHERE symbol = ? AND volatility = ?"
	var args []interface{}
	args = append(args, symbol, interval)

	if hasStartTime {
		// 关键点：为了保证前 9 行的 MA10 指标计算精准，底层查询必须包含前 9 根 K 线的历史数据作为预热窗口
		paddedStartTime := startTime - (9 * periodSeconds)
		baseWhere += " AND start_time >= ?"
		args = append(args, paddedStartTime)
	}
	if hasEndTime {
		baseWhere += " AND start_time <= ?"
		args = append(args, endTime)
	}

	// 3. 动态拼接完整 SQL 字符串
	// 并在最外层 SELECT 中，使用 WHERE start_time >= ? 剔除掉用于预热指标的富余历史数据
	sql := fmt.Sprintf(`
WITH base_data AS
    (
        SELECT
            symbol,
            period,
            volatility,
            start_time,
            dt,
            open,
            high,
            low,
            close,
            volume,
            count,
            round(volume / count, 6) AS vd,
            round(avg(volume / count) OVER (PARTITION BY symbol, period, volatility ORDER BY start_time ASC ROWS BETWEEN 9 PRECEDING AND CURRENT ROW), 6) AS ma10
        FROM agg_binance_futures_kline
        %s
        ORDER BY start_time DESC
        LIMIT ?
    )
SELECT
    *,
    round(ifNull(divideOrNull(vd, ma10), 0), 6) AS ratio
FROM base_data
WHERE 1=1
`, baseWhere)

	// 为 base_data 的 LIMIT 填充参数（传入 limit + 15 确保包含用于预热的数据行数）
	args = append(args, limit+15)

	// 外层过滤：如果传了 startTime，需要在此处将用于计算均线而多拉取的 9 条预热数据精准过滤掉
	if hasStartTime {
		sql += " AND start_time >= ?"
		args = append(args, startTime)
	}

	// 补全最外层的排序和 ClickHouse 专属安全配置
	sql += `
ORDER BY
    symbol ASC,
    period ASC,
    start_time ASC
LIMIT ?
SETTINGS
    compile_expressions = 0,
    compile_sort_description = 0;
`
	// 填充最终外层 SELECT 的 LIMIT 参数
	args = append(args, limit)

	// 4. 执行 GORM 原生 SQL 查询
	if err := db.Raw(sql, args...).Scan(&klines).Error; err != nil {
		return nil, err
	}
	return klines, nil
}

func QueryVolatilityWindow(db *gorm.DB, symbol, interval string) ([]model.AggBinanceFutureKline, error) {
	query := db.Table("agg_binance_futures_kline").
		Where("symbol = ?", symbol).
		Where("volatility = ?", interval).Order("start_time DESC").Limit(10)
	var points []model.AggBinanceFutureKline
	if err := query.Find(&points).Error; err != nil {
		return nil, err
	}
	return points, nil
}

// allVolatilityPointsHandler handles GET /fapi/v1/volatility/all requests.
// It returns all volatility points for all symbols and all volatility levels.
type allVolatilityPointsHandler struct {
	storage model.Storage
}

// ServeHTTP implements the http.Handler interface.
func (h *allVolatilityPointsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, -1102, "Method not allowed.")
		return
	}

	db := h.storage.GetDB()
	if db == nil {
		writeAPIError(w, -1001, "Internal error: database not available.")
		return
	}

	points, err := GetAllVolatilityPoints(db)
	if err != nil {
		fmt.Printf("[volatility-api] GetAllVolatilityPoints query error: %v\n", err)
		writeAPIError(w, -1001, "Internal error: failed to query all volatility points.")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(points); err != nil {
		fmt.Printf("[volatility-api] response encoding error: %v\n", err)
	}
}

func GetAllVolatilityPoints(db *gorm.DB) ([]map[string]any, error) {
	sql := `
WITH base_data AS
(
    SELECT
        symbol,
        period,
        volatility,
        start_time,
        dt,
        open,
        high,
        low,
        close,
        volume,
        count,
        round(volume / count, 6) AS vd,
        round(
            avg(volume / count) OVER (
                PARTITION BY symbol, period, volatility
                ORDER BY start_time ASC
                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
            ),
            6
        ) AS ma10
    FROM agg_binance_futures_kline
),
final_data AS
(
    SELECT
        *,
        round(ifNull(divideOrNull(vd, ma10), 0), 6) AS ratio
    FROM base_data
)
SELECT *
FROM final_data
ORDER BY
    symbol ASC,
    period ASC,
    volatility ASC,
    start_time ASC
SETTINGS
    compile_expressions = 0,
    compile_sort_description = 0;
`
	var points []map[string]any
	if err := db.Raw(sql).Scan(&points).Error; err != nil {
		return nil, err
	}
	return points, nil
}
