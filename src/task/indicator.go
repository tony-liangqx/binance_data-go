package task

import (
	"fmt"
)

// SMAIndicator implements Simple Moving Average indicator.
// It uses a sliding window of close prices from aggregated klines.
type SMAIndicator struct {
	name   string
	period string // aggregation period, e.g. "5m"
	window int    // SMA window size (number of aggregated klines)

	// sliding window buffer of close prices
	values []float64

	// current SMA value
	current float64

	// ready is true after ColdStart completes
	ready bool
}

// NewSMAIndicator creates a new SMA indicator.
// window is the number of aggregated klines for the moving average (e.g. 14).
func NewSMAIndicator(period string, window int) *SMAIndicator {
	return &SMAIndicator{
		name:   fmt.Sprintf("SMA(%d)", window),
		period: period,
		window: window,
		values: make([]float64, 0, window),
	}
}

// Name returns the indicator name.
func (s *SMAIndicator) Name() string {
	return s.name
}

// ColdStart initializes the indicator with historical data.
// For now this is a mock — in production it would query the DB for
// the last N aggregated klines for (symbol, period).
func (s *SMAIndicator) ColdStart(symbol, period string) {
	// TODO: fetch actual historical aggregated klines from database
	// Mock: generate a realistic price series from a base price
	basePrice := 50000.0
	s.values = make([]float64, 0, s.window)
	for i := 0; i < s.window; i++ {
		// Simulate slight price variation
		v := basePrice + float64(i)*10.0 + float64(i%5)*2.0
		s.values = append(s.values, v)
	}

	// Compute initial SMA
	sum := 0.0
	for _, v := range s.values {
		sum += v
	}
	s.current = sum / float64(len(s.values))
	s.ready = true

	fmt.Printf("[indicator] SMA(%d) cold start for %s/%s: initial value=%.2f (window=%d)\n",
		s.window, symbol, period, s.current, len(s.values))
}

// Calculate updates the SMA with a new aggregated kline using a sliding window.
func (s *SMAIndicator) Calculate(kline *AggregatedKline) {
	if !s.ready {
		// Auto cold-start if not ready
		s.ColdStart(kline.Symbol, kline.Period)
	}

	// Slide the window: remove oldest, append newest
	if len(s.values) >= s.window {
		s.values = append(s.values[1:], kline.Close)
	} else {
		s.values = append(s.values, kline.Close)
	}

	// Recompute SMA
	sum := 0.0
	for _, v := range s.values {
		sum += v
	}
	s.current = sum / float64(len(s.values))

	fmt.Printf("[indicator] SMA(%d) calculated for %s/%s start=%d: value=%.2f\n",
		s.window, kline.Symbol, kline.Period, kline.StartTime, s.current)
}

// GetValue returns the current SMA value.
func (s *SMAIndicator) GetValue() float64 {
	return s.current
}

// VolumeDensityIndicator calculates the cumulative volume / count ratio
// across all aggregated klines processed by a priceChangeAggregator.
// The value represents the average volume per 1m point in the aggregation window.
type VolumeDensityIndicator struct {
	name   string
	period string

	// cumulative running totals
	totalVolume float64
	totalCount  int

	// current ratio value
	current float64
}

// NewVolumeDensityIndicator creates a new volume_density indicator.
func NewVolumeDensityIndicator(period string) *VolumeDensityIndicator {
	return &VolumeDensityIndicator{
		name:   "volume_density",
		period: period,
	}
}

// Name returns the indicator name.
func (v *VolumeDensityIndicator) Name() string {
	return v.name
}

// ColdStart initializes the indicator with historical data.
// For now this is a no-op placeholder; in production it would fetch
// historical aggregated klines from the database to pre-populate the
// cumulative totals.
func (v *VolumeDensityIndicator) ColdStart(symbol, period string) {
	// TODO: load historical aggregated klines from DB and pre-populate
	// totalVolume / totalCount
	v.current = 0
	fmt.Printf("[indicator] VolumeDensity cold start for %s/%s: initial value=%.4f\n",
		symbol, period, v.current)
}

// Calculate updates the cumulative volume / count ratio with a new aggregated kline.
func (v *VolumeDensityIndicator) Calculate(kline *AggregatedKline) {
	v.totalVolume += kline.Volume
	v.totalCount += kline.Count

	if v.totalCount > 0 {
		v.current = v.totalVolume / float64(v.totalCount)
	} else {
		v.current = 0
	}

	fmt.Printf("[indicator] VolumeDensity calculated for %s/%s start=%d: value=%.4f (totalVolume=%.2f, totalCount=%d)\n",
		kline.Symbol, kline.Period, kline.StartTime, v.current, v.totalVolume, v.totalCount)
}

// GetValue returns the current volume_density value.
func (v *VolumeDensityIndicator) GetValue() float64 {
	return v.current
}

// compile-time interface checks
var _ IIndicator = (*SMAIndicator)(nil)
var _ IIndicator = (*VolumeDensityIndicator)(nil)
