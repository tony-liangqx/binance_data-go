package task

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"binance.data.sync/src/model"
)

// klinesHandler handles GET /fapi/v1/klines requests.
// It queries historical kline data from the database and returns it in a format
// compatible with Binance Futures REST API.
type klinesHandler struct {
	storage model.Storage
}

// klineRecord represents a single kline in the Binance API response format.
type klineRecord []any

// newKlineRecord builds a Binance-compatible kline record array from a model row.
// Format: [openTime, open, high, low, close, volume, closeTime, quoteVolume, trades, takerBuyBaseVol, takerBuyQuoteVol, ignore]
func newKlineRecord(k *model.BinanceFutureKline) klineRecord {
	return klineRecord{
		k.StartTime,                         // 0: Open time
		formatFloat(k.Open),                 // 1: Open
		formatFloat(k.High),                 // 2: High
		formatFloat(k.Low),                  // 3: Low
		formatFloat(k.Close),                // 4: Close
		formatFloat(k.Volume),               // 5: Volume
		k.CloseTime,                         // 6: Close time
		formatFloat(k.QuoteAssetVolume),     // 7: Quote asset volume
		k.Trades,                            // 8: Number of trades
		formatFloat(k.ActiveBuyVolume),      // 9: Taker buy base asset volume
		formatFloat(k.ActiveBuyQuoteVolume), // 10: Taker buy quote asset volume
		"0",                                 // 11: Ignore
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

	// Build the SQL query based on provided parameters
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
		fmt.Printf("[klines-api] database query error: %v\n", err)
		writeAPIError(w, -1001, "Internal error: failed to query kline data.")
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
