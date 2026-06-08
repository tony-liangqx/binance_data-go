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

// klinesHandler handles GET /fapi/v1/klines requests.
// It queries historical kline data from the database and returns it in a format
// compatible with Binance Futures REST API.
//
// The database stores only "1m" klines. When a different interval is requested,
// the data is aggregated on-the-fly using ClickHouse's aggregation functions.
type klinesHandler struct {
	storage model.Storage
}

// klineRecord represents a single kline in the Binance API response format.
type klineRecord []any

// newKlineRecord builds a Binance-compatible kline record array from a model row.
// Format: [openTime, open, high, low, close, volume, closeTime, quoteVolume, trades, takerBuyBaseVol, takerBuyQuoteVol, ignore]
func newKlineRecord(k *model.BinanceFutureKline) klineRecord {
	return klineRecord{
		k.StartTime,                             // 0: Open time
		formatFloat(k.Open),                     // 1: Open
		formatFloat(k.High),                     // 2: High
		formatFloat(k.Low),                      // 3: Low
		formatFloat(k.Close),                    // 4: Close
		formatFloat(k.Volume),                   // 5: Volume
		k.CloseTime,                             // 6: Close time
		formatFloat(k.QuoteAssetVolume),         // 7: Quote asset volume
		k.Trades,                                // 8: Number of trades
		formatFloat(k.TakerBuyBaseAssetVolume),  // 9: Taker buy base asset volume
		formatFloat(k.TakerBuyQuoteAssetVolume), // 10: Taker buy quote asset volume
		"0",                                     // 11: Ignore
	}
}

// formatFloat converts a float64 to a Binance-style decimal string.
// It removes unnecessary trailing zeros to match Binance's format.
func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// ServeHTTP implements the http.Handler interface.
func (h *klinesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	// Validate interval format (must match known Binance intervals)
	if !isValidInterval(interval) {
		writeAPIError(w, -1100, fmt.Sprintf("Invalid interval: %s", interval))
		return
	}

	// Parse optional parameters
	// Default: last 500 klines if no time range specified
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

	var klines []model.BinanceFutureKline
	var err error

	if interval == "1m" {
		// Direct query for 1m data (the only interval natively stored)
		klines, err = h.queryDirect(db, symbol, interval, startTime, endTime, hasStartTime, hasEndTime, limit)
	} else {
		// Aggregate from 1m data for other intervals using ClickHouse functions
		klines, err = h.queryAggregated(db, symbol, interval, startTime, endTime, hasStartTime, hasEndTime, limit)
	}

	if err != nil {
		fmt.Printf("[klines-api] query error: %v\n", err)
		writeAPIError(w, -1001, fmt.Sprintf("Internal error: failed to query kline data. %s", err.Error()))
		return
	}

	// Convert to Binance response format
	records := make([]klineRecord, 0, len(klines))
	for i := range klines {
		records = append(records, newKlineRecord(&klines[i]))
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(records); err != nil {
		fmt.Printf("[klines-api] response encoding error: %v\n", err)
	}
}

// queryDirect executes a simple query against binance_futures_kline for 1m data.
func (h *klinesHandler) queryDirect(db *gorm.DB, symbol, interval string, startTime, endTime int64, hasStartTime, hasEndTime bool, limit int) ([]model.BinanceFutureKline, error) {
	var klines []model.BinanceFutureKline

	query := db.Table("binance_futures_kline").
		Where("symbol = ?", symbol).
		Where("period = ?", interval)

	if hasStartTime {
		query = query.Where("start_time >= ?", startTime)
	}
	if hasEndTime {
		query = query.Where("start_time <= ?", endTime)
	}

	query = query.Order("start_time ASC").Limit(limit)

	if err := query.Scan(&klines).Error; err != nil {
		return nil, err
	}
	return klines, nil
}

// queryAggregated builds a ClickHouse aggregation query that groups 1m data
// into the target interval buckets using ClickHouse's native aggregate functions.
//
// A subquery is used to pre-compute the bucket expression, avoiding column alias
// conflicts with the raw start_time column used inside argMin/argMax aggregate functions.
func (h *klinesHandler) queryAggregated(db *gorm.DB, symbol, interval string, startTime, endTime int64, hasStartTime, hasEndTime bool, limit int) ([]model.BinanceFutureKline, error) {
	var klines []model.BinanceFutureKline

	// Build the ClickHouse SQL expression for bucketing start_time by the target interval.
	// For most intervals we use integer division (intDiv), which is fast and precise.
	// For weeks and months we use ClickHouse's calendar-aware functions.
	bucketExpr := buildBucketExpr(interval)

	// Build WHERE clause for filtering 1m source data
	whereClause := "symbol = ? AND period = '1m'"
	args := []any{symbol}
	if hasStartTime {
		whereClause += " AND start_time >= ?"
		args = append(args, startTime)
	}
	if hasEndTime {
		whereClause += " AND start_time <= ?"
		args = append(args, endTime)
	}

	// Use a subquery to avoid alias conflicts between the bucketed start_time expression
	// and the raw start_time column needed inside argMin/argMax aggregate functions.
	//
	// Inner query: selects raw columns plus a bucket_start computed column.
	// Outer query: aggregates by bucket_start, referencing raw start_time unambiguously.
	//
	// Aggregation logic:
	//   - open:   argMin(open, start_time) — first 1m open in the bucket
	//   - high:   max(high)                  — highest high in the bucket
	//   - low:    min(low)                   — lowest low in the bucket
	//   - close:  argMax(close, start_time)  — last 1m close in the bucket
	//   - volume: sum(volume)                — total volume
	//   - trades: sum(trades)                — total trade count
	//   - dt, close_time: from the last 1m kline in the bucket
	sql := fmt.Sprintf(`
	SELECT
		symbol,
		? AS period,
		bucket_start AS start_time,
		argMin(open, start_time) AS open,
		max(high) AS high,
		min(low) AS low,
		argMax(close, start_time) AS close,
		sum(volume) AS volume,
		sum(quote_asset_volume) AS quote_asset_volume,
		sum(trades) AS trades,
		sum(taker_buy_base_asset_volume) AS taker_buy_base_asset_volume,
		sum(taker_buy_quote_asset_volume) AS taker_buy_quote_asset_volume,
		argMax(close_time, start_time) AS close_time,
		argMax(dt, start_time) AS dt
	FROM (
		SELECT
			symbol,
			%s AS bucket_start,
			start_time, open, high, low, close, volume,
			quote_asset_volume, trades, taker_buy_base_asset_volume,
			taker_buy_quote_asset_volume, close_time, dt
		FROM binance_futures_kline
		WHERE %s
	)
	GROUP BY symbol, bucket_start
	ORDER BY bucket_start ASC
	LIMIT ?
`, bucketExpr, whereClause)
	fmt.Printf("sql :%s\n", sql)
	// Args order: [period, whereClause args..., limit]
	queryArgs := make([]any, 0, len(args)+2)
	queryArgs = append(queryArgs, interval) // for `? AS period`
	queryArgs = append(queryArgs, args...)  // for WHERE clause
	queryArgs = append(queryArgs, limit)    // for LIMIT ?

	if err := db.Debug().Raw(sql, queryArgs...).Scan(&klines).Error; err != nil {
		return nil, err
	}
	return klines, nil
}

// buildBucketExpr returns a ClickHouse SQL expression that rounds a 1m start_time
// (in milliseconds) down to the start of the bucket for the given interval.
//
// For most intervals we use integer division (intDiv), which is the fastest approach.
// For "1w" (week) we use toStartOfWeek to align to Monday boundaries.
// For "1M" (month) we use toStartOfMonth for calendar-month boundaries.
func buildBucketExpr(interval string) string {
	switch interval {
	case "1w":
		// Align to Monday 00:00:00 UTC (mode 1 = Monday as first day of week)
		return "toUnixTimestamp64Milli(toStartOfWeek(toDateTime64(intDiv(start_time, 1000), 3), 1, 'UTC'))"
	case "1M":
		// Align to the first day of the month at 00:00:00 UTC
		return "toUnixTimestamp64Milli(toStartOfMonth(toDateTime64(intDiv(start_time, 1000), 3), 'UTC'))"
	default:
		// For all fixed-length intervals, use integer division from epoch.
		// This works correctly because epoch (1970-01-01 00:00:00 UTC) is aligned
		// to all Binance interval boundaries (1m, 5m, 15m, 1h, 1d, etc.).
		ms := intervalToMilliseconds(interval)
		return fmt.Sprintf("intDiv(start_time, %d) * %d", ms, ms)
	}
}

// intervalToMilliseconds converts a Binance interval string to its duration in milliseconds.
func intervalToMilliseconds(interval string) int64 {
	switch interval {
	case "1m":
		return 60000
	case "3m":
		return 180000
	case "5m":
		return 300000
	case "15m":
		return 900000
	case "30m":
		return 1800000
	case "1h":
		return 3600000
	case "2h":
		return 7200000
	case "4h":
		return 14400000
	case "6h":
		return 21600000
	case "8h":
		return 28800000
	case "12h":
		return 43200000
	case "1d":
		return 86400000
	case "3d":
		return 259200000
	case "1w":
		return 604800000
	}
	return 0
}

// isValidInterval checks if the interval is one of the known Binance Futures intervals.
func isValidInterval(interval string) bool {
	switch interval {
	case "1m", "3m", "5m", "15m", "30m",
		"1h", "2h", "4h", "6h", "8h", "12h",
		"1d", "3d", "1w", "1M":
		return true
	}
	return false
}

// binanceAPIError is the JSON error response format used by Binance.
type binanceAPIError struct {
	Code int64  `json:"code"`
	Msg  string `json:"msg"`
}

// writeAPIError writes a Binance-compatible error response.
func writeAPIError(w http.ResponseWriter, code int64, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(binanceAPIError{Code: code, Msg: msg})
}
