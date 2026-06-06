package model

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// VolatilityDataWriter accumulates 1m kline points and produces an aggregated
// kline whenever the price change percentage exceeds 0.01 %.
//
// The trigger condition is:
//
//	changePct := (math.Abs(lastClose-firstClose) / firstClose) * 100
//	triggered when changePct > 0.01
//
// This is a price - driven aggregator, unlike the count - based symbolAggregator.
type VolatilityDataWriter struct {
	symbol     string
	period     string
	volatility float64

	// storage persists aggregated kline data to the database
	storage Storage

	// previous aggregated kline ("上一个数据点")
	prevAggPoint *AggBinanceSpotKline
}

// NewVolatilityDataWriter creates a new price - change - driven aggregator for
// the given symbol / period.
func NewVolatilityDataWriter(symbol string, volatility float64, storage Storage) *VolatilityDataWriter {
	vName := strconv.Itoa(int(volatility * 10))
	lastPoint, err := storage.GetLastVolatilityPoint(symbol, "1m", strconv.Itoa(int(volatility*10)))
	if err != nil {
		fmt.Printf("[volatility_data_writer(%s %s): %s\n", symbol, vName, err.Error())
	}
	if lastPoint != nil {
		fmt.Printf("[volatility_data_writer(%s %s)] loaded last point: start_time: %d\n", symbol, vName, lastPoint.StartTime)
	}
	return &VolatilityDataWriter{
		symbol:       symbol,
		volatility:   volatility,
		storage:      storage,
		prevAggPoint: lastPoint,
	}
}

// Symbol returns the trading symbol this aggregator tracks.
func (a *VolatilityDataWriter) Symbol() string { return a.symbol }

// Period returns the aggregation period.
func (a *VolatilityDataWriter) Volatility() string { return strconv.Itoa(int(a.volatility * 10)) }

// Add inserts a 1m point into the aggregator. When the price change percentage
// exceeds 0.01 %, it produces an aggregated kline, runs all indicators, and
// returns the result. Returns nil if the threshold has not been reached.
func (a *VolatilityDataWriter) Add(point *SpotKlinePoint) (*AggregatedKline, error) {
	if a.prevAggPoint == nil {
		// First point of a new window: initialize all state
		a.prevAggPoint = &AggBinanceSpotKline{
			Symbol:           point.Symbol,
			Period:           point.Period,
			StartTime:        point.StartTime,
			DateTime:         DateTimeMillis(point.DateTime),
			Open:             point.Open,
			High:             point.High,
			Low:              point.Low,
			Close:            point.Close,
			Volume:           point.Volume,
			CloseTime:        point.CloseTime,
			QuoteAssetVolume: point.QuoteAssetVolume,
			Trades:           point.Trades,
		}
		return a.finalize(a.prevAggPoint)
	}

	changePct := (math.Abs(a.prevAggPoint.Close-point.Close) / point.Close) * 100

	if changePct > a.volatility {
		point := &AggBinanceSpotKline{
			Symbol:           point.Symbol,
			Period:           point.Period,
			Volatility:       a.Volatility(),
			StartTime:        point.StartTime,
			DateTime:         DateTimeMillis(point.DateTime),
			Open:             point.Open,
			High:             point.High,
			Low:              point.Low,
			Close:            point.Close,
			Volume:           point.Volume,
			CloseTime:        point.CloseTime,
			QuoteAssetVolume: point.QuoteAssetVolume,
			Trades:           point.Trades,
		}
		return a.finalize(point)
	}

	return nil, nil
}

// finalize builds the aggregated kline, resets the window, and runs indicators.
func (a *VolatilityDataWriter) finalize(point *AggBinanceSpotKline) (*AggregatedKline, error) {
	if a.prevAggPoint == nil {
		return nil, nil
	}

	agg := &AggregatedKline{
		Symbol:           point.Symbol,
		Period:           point.Period,
		Volatility:       a.Volatility(),
		StartTime:        point.StartTime,
		Open:             point.Open,
		High:             point.High,
		Low:              point.Low,
		Close:            point.Close,
		Volume:           point.Volume,
		CloseTime:        point.CloseTime,
		QuoteAssetVolume: point.QuoteAssetVolume,
		Trades:           point.Trades,
		Indicators:       make(map[string]any),
	}

	// 写入数据库
	if err := a.saveAggregated(point); err != nil {
		fmt.Printf("[pubsub] failed to save aggregated kline: %v\n", err)
		return nil, err
	}
	fmt.Printf("[volatility_data_writer] aggregated %s/%s: start=%d -> end=%d, changePct=%.4f%%\n",
		a.symbol, a.Volatility(), agg.StartTime, agg.CloseTime,
		(math.Abs(a.prevAggPoint.Close-point.Close)/point.Close)*100)
	a.prevAggPoint = &AggBinanceSpotKline{
		Symbol:           point.Symbol,
		Period:           point.Period,
		Volatility:       a.Volatility(),
		StartTime:        point.StartTime,
		DateTime:         DateTimeMillis(point.DateTime),
		Open:             point.Open,
		High:             point.High,
		Low:              point.Low,
		Close:            point.Close,
		Volume:           point.Volume,
		CloseTime:        point.CloseTime,
		QuoteAssetVolume: point.QuoteAssetVolume,
		Trades:           point.Trades,
	}
	return agg, nil
}

// saveAggregated writes the aggregated kline to the AggBinanceSpotKline table.
func (a *VolatilityDataWriter) saveAggregated(agg *AggBinanceSpotKline) error {
	if a.storage == nil {
		return nil
	}

	agg.DateTime = DateTimeMillis(time.Now().UnixMilli())

	if err := a.storage.CommitAggKline(agg); err != nil {
		fmt.Printf("[pubsub] failed to save aggregated kline: %v\n", err)
		return err
	} else {
		fmt.Printf("[pubsub] saved aggregated kline: %s %s start=%d\n",
			agg.Symbol, agg.Period, agg.StartTime)
	}
	return nil
}
