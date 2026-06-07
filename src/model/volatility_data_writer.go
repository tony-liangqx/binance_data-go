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
	kind       string

	// storage persists aggregated kline data to the database
	storage Storage

	// count of 1m points accumulated in the current aggregation window
	count int

	// running aggregates for the current window
	high             float64
	low              float64
	volume           float64
	quoteAssetVolume float64
	trades           uint32

	// previous aggregated kline ("上一个数据点")
	firstPoint *AggBinanceSpotKline
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
		return &VolatilityDataWriter{
			symbol:     symbol,
			volatility: volatility,
			kind:       "volatility",
			storage:    storage,
			firstPoint: lastPoint,
		}
	}
	return &VolatilityDataWriter{
		symbol:     symbol,
		volatility: volatility,
		storage:    storage,
		firstPoint: nil,
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
	if a.firstPoint == nil {
		// First point of a new window: initialize all state
		a.firstPoint = &AggBinanceSpotKline{
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

		a.high = point.High
		a.low = point.Low

		a.volume = point.Volume
		a.quoteAssetVolume = point.QuoteAssetVolume
		a.trades = point.Trades
		a.count = 1

		agg, err := a.finalize(a.firstPoint)
		if err != nil {
			fmt.Printf("[volatility_data_writer] failed to save aggregated kline: %v\n", err)
			return nil, err
		}
		fmt.Printf("[volatility_data_writer] first aggregated %s/%s: %d points, start=%d -> end=%d, changePct=%.4f%%\n",
			a.symbol, a.Volatility(), a.count, agg.StartTime, agg.CloseTime,
			(math.Abs(a.firstPoint.Close-point.Close)/point.Close)*100)
		return agg, nil

	}

	a.count++

	if point.High > a.high {
		a.high = point.High
	}
	if point.Low < a.low {
		a.low = point.Low
	}
	a.volume += point.Volume
	a.quoteAssetVolume += point.QuoteAssetVolume
	a.trades += point.Trades

	changePct := (math.Abs(a.firstPoint.Close-point.Close) / point.Close) * 100

	if changePct > a.volatility {
		newAgg := &AggBinanceSpotKline{
			Symbol:     a.symbol,
			Period:     a.period,
			Volatility: a.Volatility(),
			StartTime:  a.firstPoint.StartTime,
			DateTime:   DateTimeMillis(point.DateTime),
			Open:       a.firstPoint.Open,
			High:       a.high,
			Low:        a.low,
			Close:      point.Close,
			CloseTime:  point.CloseTime,
			// 计算聚合值
			Volume:           a.volume,
			QuoteAssetVolume: a.quoteAssetVolume,
			Trades:           a.trades,
		}
		agg, err := a.finalize(newAgg)
		if err != nil {
			return nil, err
		}

		fmt.Printf("[volatility_data_writer] aggregated %s/%s: %d points, start=%d -> end=%d, changePct=%.4f%%\n",
			a.symbol, a.Volatility(), a.count, agg.StartTime, agg.CloseTime,
			(math.Abs(a.firstPoint.Close-point.Close)/point.Close)*100)

		// Reset window state, using the current point as the start of the next window
		a.firstPoint = &AggBinanceSpotKline{
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

		a.high = point.High
		a.low = point.Low

		a.volume = point.Volume
		a.quoteAssetVolume = point.QuoteAssetVolume
		a.trades = point.Trades
		a.count = 1

		return agg, nil
	}

	return nil, nil
}

// finalize builds the aggregated kline, resets the window, and runs indicators.
func (a *VolatilityDataWriter) finalize(point *AggBinanceSpotKline) (*AggregatedKline, error) {
	// 写入数据库
	if err := a.saveAggregated(point); err != nil {
		return nil, err
	}

	// 返回的数据
	agg := &AggregatedKline{
		Symbol:     point.Symbol,
		Period:     point.Period,
		Kind:       a.kind,
		Volatility: a.Volatility(),
		StartTime:  point.StartTime,
		Open:       point.Open,
		High:       point.High,
		Low:        point.Low,
		Close:      point.Close,

		// 计算聚合值
		Volume:           point.Volume,
		CloseTime:        point.CloseTime,
		QuoteAssetVolume: point.QuoteAssetVolume,
		Trades:           point.Trades,

		Count:      a.count,
		Indicators: make(map[string]any),
	}

	return agg, nil
}

// saveAggregated writes the aggregated kline to the AggBinanceSpotKline table.
func (a *VolatilityDataWriter) saveAggregated(agg *AggBinanceSpotKline) error {
	agg.DateTime = DateTimeMillis(time.Now().UnixMilli())

	if err := a.storage.CommitAggKline(agg); err != nil {
		return err
	}
	return nil
}
