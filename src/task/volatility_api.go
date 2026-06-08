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
	Period                   string  `json:"-"`
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
}

// newVolatilityRecord builds a volatilityRecord from a model row.
func newVolatilityRecord(k *model.AggBinanceFutureKline) volatilityRecord {
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
func (h *volatilityHandler) queryVolatility(db *gorm.DB, symbol, interval string, startTime, endTime int64, hasStartTime, hasEndTime bool, limit int) ([]model.AggBinanceFutureKline, error) {
	var klines []model.AggBinanceFutureKline

	// The "interval" parameter maps to the "volatility" column in the DB.
	// The "period" column stores the original kline period (e.g., "1m").
	query := db.Table("agg_binance_futures_kline").
		Where("symbol = ?", symbol).
		Where("volatility = ?", interval)

	if hasStartTime {
		query = query.Where("start_time >= ?", startTime)
	}
	if hasEndTime {
		query = query.Where("start_time <= ?", endTime)
	}

	query = query.Order("start_time DESC").Limit(limit)

	if err := query.Scan(&klines).Error; err != nil {
		return nil, err
	}
	return klines, nil
}
