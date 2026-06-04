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

// compile-time interface check
var _ IIndicator = (*SMAIndicator)(nil)
