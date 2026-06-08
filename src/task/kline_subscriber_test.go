package task

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"binance.data.sync/src/model"
	"gorm.io/gorm"

	"github.com/adshao/go-binance/v2/futures"
)

// mockStorage implements model.Storage in-memory for integration testing.
type mockStorage struct {
	mu sync.RWMutex

	// klines keyed by "symbol|period", ordered by append
	klines map[string][]*model.FutureKlinePoint

	// aggKlines keyed by "symbol|period|volatility", ordered by append
	aggKlines map[string][]*model.AggBinanceFutureKline

	// Track how many times each method was called for assertion
	commitCalls    int
	commitAggCalls int
	lastTimeCalls  int
	lastVolCalls   int
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		klines:    make(map[string][]*model.FutureKlinePoint),
		aggKlines: make(map[string][]*model.AggBinanceFutureKline),
	}
}

func (m *mockStorage) Commit(point *model.FutureKlinePoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitCalls++
	key := point.Symbol + "|" + point.Period
	m.klines[key] = append(m.klines[key], point)
	return nil
}

func (m *mockStorage) CommitAggKline(kline *model.AggBinanceFutureKline) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitAggCalls++
	key := kline.Symbol + "|" + kline.Period + "|" + kline.Volatility
	m.aggKlines[key] = append(m.aggKlines[key], kline)
	return nil
}

func (m *mockStorage) GetLastTimeStamp(symbol string, period string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastTimeCalls++
	key := symbol + "|" + period
	entries := m.klines[key]
	if len(entries) == 0 {
		return 0, nil
	}
	// Entries are appended in order, so the last one has the latest timestamp
	return entries[len(entries)-1].StartTime, nil
}

func (m *mockStorage) GetLastVolatilityPoint(symbol string, period string, volatility string) (*model.AggBinanceFutureKline, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastVolCalls++
	key := symbol + "|" + period + "|" + volatility
	entries := m.aggKlines[key]
	if len(entries) == 0 {
		return nil, nil
	}
	return entries[len(entries)-1], nil
}

func (m *mockStorage) GetDB() *gorm.DB {
	return nil
}

// snapAggKlines returns a snapshot of stored aggregated klines for a given
// symbol/period, across all volatility levels.
func (m *mockStorage) snapAggKlines(symbol, period string) []*model.AggBinanceFutureKline {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Collect all volatility levels under this symbol|period prefix
	prefix := symbol + "|" + period + "|"
	var all []*model.AggBinanceFutureKline
	for key, entries := range m.aggKlines {
		if strings.HasPrefix(key, prefix) {
			all = append(all, entries...)
		}
	}
	// Deep copy
	cp := make([]*model.AggBinanceFutureKline, len(all))
	for i, p := range all {
		cp[i] = &model.AggBinanceFutureKline{
			Symbol:           p.Symbol,
			Period:           p.Period,
			Volatility:       p.Volatility,
			StartTime:        p.StartTime,
			DateTime:         p.DateTime,
			Open:             p.Open,
			High:             p.High,
			Low:              p.Low,
			Close:            p.Close,
			Volume:           p.Volume,
			CloseTime:        p.CloseTime,
			QuoteAssetVolume: p.QuoteAssetVolume,
			Trades:           p.Trades,
		}
	}
	return cp
}

// snapKlines returns a snapshot of stored klines for a given symbol/period.
func (m *mockStorage) snapKlines(symbol, period string) []*model.FutureKlinePoint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := symbol + "|" + period
	cp := make([]*model.FutureKlinePoint, len(m.klines[key]))
	for i, p := range m.klines[key] {
		cp[i] = &model.FutureKlinePoint{
			Symbol:           p.Symbol,
			Period:           p.Period,
			StartTime:        p.StartTime,
			DateTime:         p.DateTime,
			Open:             p.Open,
			High:             p.High,
			Low:              p.Low,
			Close:            p.Close,
			Volume:           p.Volume,
			CloseTime:        p.CloseTime,
			QuoteAssetVolume: p.QuoteAssetVolume,
			Trades:           p.Trades,
		}
	}
	return cp
}

// TestSubscriber_HandleKline_ContiguousData verifies that feeding 100
// contiguous 1m kline points into the Subscriber results in all 100 points
// being persisted (no gaps, no duplicates, no history sync triggered).
func TestSubscriber_HandleKline_ContiguousData(t *testing.T) {
	storage := newMockStorage()
	symbol := "BTCUSDT"
	period := "1m"

	sub := NewSubscriber(storage, symbol, period)

	// Also verify the pointChan receives aggregated klines
	pointCh := make(chan *model.AggregatedFutureKline, 50)
	sub.SetPointChan(pointCh)

	// --- Generate 100 contiguous 1m test points ---
	// Use a small, steady price movement (0.01 per minute) so that volatility
	// thresholds are never crossed after the initial aggregation — this keeps
	// the test deterministic.
	baseTime := int64(1700000000000) // 2023-11-14 22:13:20 UTC
	startPrice := 50000.0
	totalPoints := 100
	volume := 100.5
	quoteVolume := 5025000.0
	trades := int64(342)

	for i := 0; i < totalPoints; i++ {
		ts := baseTime + int64(i)*60000
		price := startPrice + float64(i)*0.01 // price rises $0.01 per minute (~0.00002%)

		kline := &futures.WsKline{
			StartTime:   ts,
			EndTime:     ts + 60000 - 1,
			Symbol:      symbol,
			Open:        fmt.Sprintf("%.2f", price),
			High:        fmt.Sprintf("%.2f", price+50),
			Low:         fmt.Sprintf("%.2f", price-50),
			Close:       fmt.Sprintf("%.2f", price+20),
			Volume:      fmt.Sprintf("%.2f", volume),
			QuoteVolume: fmt.Sprintf("%.2f", quoteVolume),
			TradeNum:    trades,
			IsFinal:     true,
		}
		sub.HandleKline(kline)
	}

	// --- Verification ---

	// 1. All 100 points committed to storage
	stored := storage.snapKlines(symbol, period)
	if len(stored) != totalPoints {
		t.Fatalf("expected %d stored klines, got %d", totalPoints, len(stored))
	}
	t.Logf("stored %d klines in mock storage", len(stored))

	// 2. Timestamps are sequential and contiguous
	for i, p := range stored {
		expectedStart := baseTime + int64(i)*60000
		if p.StartTime != expectedStart {
			t.Errorf("point %d: expected StartTime %d, got %d", i, expectedStart, p.StartTime)
		}
		if p.CloseTime != expectedStart+60000-1 {
			t.Errorf("point %d: expected CloseTime %d, got %d", i, expectedStart+60000-1, p.CloseTime)
		}
		// Verify the subscriber's internal timestamp was updated
		if i == len(stored)-1 {
			if ts := sub.GetTimeStamp(); ts != expectedStart {
				t.Errorf("expected GetTimeStamp()=%d, got %d", expectedStart, ts)
			}
		}
	}

	// 3. Price data matches
	for i, p := range stored {
		price := startPrice + float64(i)*0.01
		if p.Open != price {
			t.Errorf("point %d: expected Open %.2f, got %.2f", i, price, p.Open)
		}
		if p.Close != price+20 {
			t.Errorf("point %d: expected Close %.2f, got %.2f", i, price+20, p.Close)
		}
		if p.High != price+50 {
			t.Errorf("point %d: expected High %.2f, got %.2f", i, price+50, p.High)
		}
		if p.Low != price-50 {
			t.Errorf("point %d: expected Low %.2f, got %.2f", i, price-50, p.Low)
		}
		if p.Volume != volume {
			t.Errorf("point %d: expected Volume %.2f, got %.2f", i, volume, p.Volume)
		}
		if p.QuoteAssetVolume != quoteVolume {
			t.Errorf("point %d: expected QuoteAssetVolume %.2f, got %.2f", i, quoteVolume, p.QuoteAssetVolume)
		}
		if p.Trades != uint32(trades) {
			t.Errorf("point %d: expected Trades %d, got %d", i, trades, p.Trades)
		}
	}

	// 4. Only the first point triggered aggregations (new prevAggPoint for each
	// VolatilityDataWriter). With tiny price increments (0.01/min), the ~0.00002%
	// change never exceeds 0.5%/1%/2% thresholds, so exactly 3 aggregated klines
	// are produced — one per volatility level.
	t.Logf("Commit calls: %d, CommitAgg calls: %d, LastTime calls: %d, LastVol calls: %d",
		storage.commitCalls, storage.commitAggCalls, storage.lastTimeCalls, storage.lastVolCalls)

	close(pointCh)
	var received []*model.AggregatedFutureKline
	for p := range pointCh {
		received = append(received, p)
	}
	if len(received) != 3 {
		t.Fatalf("expected 3 aggregated klines on pointChan, got %d", len(received))
	}
	for _, p := range received {
		if p.StartTime != baseTime {
			t.Errorf("aggregated point expected StartTime %d, got %d", baseTime, p.StartTime)
		}
		if p.Symbol != symbol {
			t.Errorf("aggregated point expected Symbol %s, got %s", symbol, p.Symbol)
		}
	}
	t.Logf("received %d aggregated klines on pointChan (%s/%s)", len(received), symbol, period)

	// 5. Verify CommitAggKline data — exactly 3 agg klines stored by the first point
	aggKlines := storage.snapAggKlines(symbol, period)
	if len(aggKlines) != 3 {
		t.Fatalf("expected 3 aggregated klines in storage, got %d", len(aggKlines))
	}
	for _, agg := range aggKlines {
		validateAggKline(t, agg, symbol, period, baseTime,
			50000.0 /* open */, 50050.0 /* high */, 49950.0 /* low */, 50020.0, /* close */
			100.5 /* volume */, 5025000.0 /* quoteVolume */, uint32(342) /* trades */)
	}
	t.Logf("all %d aggregated klines validated in storage", len(aggKlines))
}

// TestSubscriber_HandleKline_NonFinalIsSkipped verifies that non-final
// klines are ignored by HandleKline.
func TestSubscriber_HandleKline_NonFinalIsSkipped(t *testing.T) {
	storage := newMockStorage()
	sub := NewSubscriber(storage, "ETHUSDT", "1m")

	baseTime := int64(1700000000000)

	// Feed a non-final kline — should be ignored
	nonFinal := &futures.WsKline{
		StartTime:   baseTime,
		EndTime:     baseTime + 60000 - 1,
		Symbol:      "ETHUSDT",
		Open:        "3000.00",
		High:        "3010.00",
		Low:         "2990.00",
		Close:       "3005.00",
		Volume:      "100.0",
		QuoteVolume: "300500.0",
		TradeNum:    100,
		IsFinal:     false,
	}
	sub.HandleKline(nonFinal)

	stored := storage.snapKlines("ETHUSDT", "1m")
	if len(stored) != 0 {
		t.Fatalf("expected 0 stored klines for non-final event, got %d", len(stored))
	}

	// Now feed a final kline — should be stored
	final := &futures.WsKline{
		StartTime:   baseTime,
		EndTime:     baseTime + 60000 - 1,
		Symbol:      "ETHUSDT",
		Open:        "3000.00",
		High:        "3010.00",
		Low:         "2990.00",
		Close:       "3005.00",
		Volume:      "100.0",
		QuoteVolume: "300500.0",
		TradeNum:    100,
		IsFinal:     true,
	}
	sub.HandleKline(final)

	stored = storage.snapKlines("ETHUSDT", "1m")
	if len(stored) != 1 {
		t.Fatalf("expected 1 stored kline after final event, got %d", len(stored))
	}
	if stored[0].StartTime != baseTime {
		t.Errorf("expected StartTime %d, got %d", baseTime, stored[0].StartTime)
	}

	// Verify CommitAggKline data — 3 agg klines from the first (and only) final point
	aggKlines := storage.snapAggKlines("ETHUSDT", "1m")
	if len(aggKlines) != 3 {
		t.Fatalf("expected 3 aggregated klines in storage, got %d", len(aggKlines))
	}
	for _, agg := range aggKlines {
		validateAggKline(t, agg, "ETHUSDT", "1m", baseTime,
			3000.0 /* open */, 3010.0 /* high */, 2990.0 /* low */, 3005.0, /* close */
			100.0 /* volume */, 300500.0 /* quoteVolume */, uint32(100) /* trades */)
	}
	t.Logf("all %d aggregated klines validated in storage", len(aggKlines))
}

// TestSubscriber_HandleKline_GapDetection verifies that a gap in timestamps
// triggers the HistorySyncer path (syncing flag set, subsequent points buffered).
//
// The trigger point itself is NOT buffered — the syncer will fetch it via REST.
// Points arriving AFTER the syncer starts ARE buffered until SyncDone is called.
func TestSubscriber_HandleKline_GapDetection(t *testing.T) {
	storage := newMockStorage()
	symbol := "SOLUSDT"
	period := "1m"

	sub := NewSubscriber(storage, symbol, period)

	baseTime := int64(1700000000000)

	// 1. First point — no lastTime yet, goes straight to savePoint
	first := buildKline(symbol, baseTime, 100.0, true)
	sub.HandleKline(first)

	stored := storage.snapKlines(symbol, period)
	if len(stored) != 1 {
		t.Fatalf("expected 1 stored kline after first point, got %d", len(stored))
	}
	t.Logf("first point stored: StartTime=%d", stored[0].StartTime)

	// Verify CommitAggKline data from the first point
	aggKlines := storage.snapAggKlines(symbol, period)
	if len(aggKlines) != 3 {
		t.Fatalf("expected 3 aggregated klines after first point, got %d", len(aggKlines))
	}
	for _, agg := range aggKlines {
		validateAggKline(t, agg, symbol, period, baseTime,
			100.0 /* open */, 100.5 /* high */, 99.5 /* low */, 100.2, /* close */
			10.0 /* volume */, 1000.0 /* quoteVolume */, uint32(50) /* trades */)
	}
	t.Logf("all %d aggregated klines validated after first point", len(aggKlines))

	// 2. Second point with a gap (2 min skip = 120000ms instead of 60000ms)
	// This triggers gap detection: sets syncing=true, launches HistorySyncer.
	// The trigger point is NOT buffered (the syncer will fetch it via REST).
	gapTime := baseTime + 2*60000 // skipped minute 2 (index 1 is missing)
	gapPoint := buildKline(symbol, gapTime, 101.0, true)
	sub.HandleKline(gapPoint)

	// Verify: only 1 committed kline (the trigger point was not saved)
	storedAfter := storage.snapKlines(symbol, period)
	if len(storedAfter) != 1 {
		t.Fatalf("expected only 1 committed kline after gap point (syncer not started), got %d", len(storedAfter))
	}

	// The trigger point is NOT buffered — it's the syncer's responsibility
	sub.mu.Lock()
	bufLen := len(sub.buffer)
	sub.mu.Unlock()
	if bufLen != 0 {
		t.Errorf("expected 0 buffered points (trigger point is for syncer), got %d", bufLen)
	}

	// Verify syncing flag was set
	if !sub.isSyncing() {
		t.Fatal("expected syncing flag to be true after gap detection")
	}

	// 3. Feed a THIRD point while syncing — this one SHOULD be buffered
	bufferedTime := gapTime + 60000 // contiguous after the trigger point
	bufferedPoint := buildKline(symbol, bufferedTime, 102.0, true)
	sub.HandleKline(bufferedPoint)

	// Should still only have 1 committed kline (buffer not drained until SyncDone)
	storedAfter2 := storage.snapKlines(symbol, period)
	if len(storedAfter2) != 1 {
		t.Fatalf("expected 1 committed kline after buffered point, got %d", len(storedAfter2))
	}

	sub.mu.Lock()
	bufLen2 := len(sub.buffer)
	sub.mu.Unlock()
	if bufLen2 != 1 {
		t.Errorf("expected 1 buffered point, got %d", bufLen2)
	}

	t.Logf("gap test: trigger at %d (not buffered), buffered at %d, buffer len=%d, syncing=%v",
		gapTime, bufferedTime, bufLen2, sub.isSyncing())
}

// TestSubscriber_SetPointChan verifies that the SetPointChan and publishPoint
// path works correctly with a buffered channel.
func TestSubscriber_SetPointChan(t *testing.T) {
	storage := newMockStorage()
	sub := NewSubscriber(storage, "DOTUSDT", "1m")

	ch := make(chan *model.AggregatedFutureKline, 10)
	sub.SetPointChan(ch)

	baseTime := int64(1700000000000)
	kline := buildKline("DOTUSDT", baseTime, 10.0, true)
	sub.HandleKline(kline)

	close(ch)
	count := 0
	for range ch {
		count++
	}
	if count == 0 {
		t.Error("expected at least 1 aggregated kline on pointChan, got 0")
	}
	t.Logf("received %d aggregated klines on pointChan", count)

	// Verify CommitAggKline data — 3 agg klines from the first point
	aggKlines := storage.snapAggKlines("DOTUSDT", "1m")
	if len(aggKlines) != 3 {
		t.Fatalf("expected 3 aggregated klines in storage, got %d", len(aggKlines))
	}
	for _, agg := range aggKlines {
		validateAggKline(t, agg, "DOTUSDT", "1m", baseTime,
			10.0 /* open */, 10.5 /* high */, 9.5 /* low */, 10.2, /* close */
			10.0 /* volume */, 100.0 /* quoteVolume */, uint32(50) /* trades */)
	}
	t.Logf("all %d aggregated klines validated in storage", len(aggKlines))
}

// TestSubscriber_ConcurrencySafety verifies that concurrent reads and writes
// to Subscriber's mutex-protected state (timestamp, buffer, syncing) do not
// cause panics or data races.
//
// The test produces all points sequentially (to avoid artificial gaps) but
// reads GetTimeStamp() concurrently from multiple goroutines throughout.
func TestSubscriber_ConcurrencySafety(t *testing.T) {
	storage := newMockStorage()
	sub := NewSubscriber(storage, "ADAUSDT", "1m")

	baseTime := int64(1700000000000)
	totalPoints := 100

	// Launch concurrent readers of GetTimeStamp while feeding points
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	// Concurrent readers — read GetTimeStamp in a tight loop
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					_ = sub.GetTimeStamp()
				}
			}
		}()
	}

	// Feed all 100 points sequentially from the main goroutine
	// Use a constant price so no volatility cross-threshold triggers occur
	// after the first point — keeping agg kline count deterministic (exactly 3).
	for i := 0; i < totalPoints; i++ {
		ts := baseTime + int64(i)*60000
		kline := buildKline("ADAUSDT", ts, 1.0, true)
		sub.HandleKline(kline)
	}

	// Stop concurrent readers
	cancel()
	wg.Wait()

	stored := storage.snapKlines("ADAUSDT", "1m")
	if len(stored) != totalPoints {
		t.Fatalf("expected %d stored klines, got %d", totalPoints, len(stored))
	}

	// Verify timestamps are in ascending order
	timestamps := make([]int64, len(stored))
	for i, p := range stored {
		timestamps[i] = p.StartTime
	}
	if !sort.SliceIsSorted(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] }) {
		t.Error("stored kline timestamps are not in ascending order")
	}

	t.Logf("concurrent safety test: stored %d klines, timestamp range [%d, %d], readers=4",
		len(stored), timestamps[0], timestamps[len(timestamps)-1])

	// Verify CommitAggKline data — 3 agg klines from the first point (i=0, price=1.0)
	aggKlines := storage.snapAggKlines("ADAUSDT", "1m")
	if len(aggKlines) != 3 {
		t.Fatalf("expected 3 aggregated klines in storage, got %d", len(aggKlines))
	}
	for _, agg := range aggKlines {
		validateAggKline(t, agg, "ADAUSDT", "1m", baseTime,
			1.0 /* open */, 1.5 /* high */, 0.5 /* low */, 1.2, /* close */
			10.0 /* volume */, 10.0 /* quoteVolume */, uint32(50) /* trades */)
	}
	t.Logf("all %d aggregated klines validated in storage", len(aggKlines))
}

// TestSubscriber_GetTimeStamp verifies that the timestamp is updated on each point.
func TestSubscriber_GetTimeStamp(t *testing.T) {
	storage := newMockStorage()
	sub := NewSubscriber(storage, "XRPUSDT", "1m")

	baseTime := int64(1700000000000)

	if ts := sub.GetTimeStamp(); ts != 0 {
		t.Errorf("expected initial timestamp 0, got %d", ts)
	}

	for i := 0; i < 5; i++ {
		ts := baseTime + int64(i)*60000
		kline := buildKline("XRPUSDT", ts, 0.5+float64(i), true)
		sub.HandleKline(kline)
		if got := sub.GetTimeStamp(); got != ts {
			t.Errorf("after point %d: expected GetTimeStamp()=%d, got %d", i, ts, got)
		}
	}
}

// validateAggKline checks that an aggregated kline matches expected source data.
// expectedPrice is the `price` argument passed to buildKline or the raw Open price.
func validateAggKline(t *testing.T, agg *model.AggBinanceFutureKline, symbol, period string, startTime int64, open, high, low, closeVal, volume, quoteVolume float64, trades uint32) {
	t.Helper()
	if agg.Symbol != symbol {
		t.Errorf("agg Symbol: expected %q, got %q", symbol, agg.Symbol)
	}
	if agg.Period != period {
		t.Errorf("agg Period: expected %q, got %q", period, agg.Period)
	}
	if agg.StartTime != startTime {
		t.Errorf("agg StartTime: expected %d, got %d", startTime, agg.StartTime)
	}
	if agg.Open != open {
		t.Errorf("agg Open: expected %.2f, got %.2f", open, agg.Open)
	}
	if agg.High != high {
		t.Errorf("agg High: expected %.2f, got %.2f", high, agg.High)
	}
	if agg.Low != low {
		t.Errorf("agg Low: expected %.2f, got %.2f", low, agg.Low)
	}
	if agg.Close != closeVal {
		t.Errorf("agg Close: expected %.2f, got %.2f", closeVal, agg.Close)
	}
	if agg.Volume != volume {
		t.Errorf("agg Volume: expected %.2f, got %.2f", volume, agg.Volume)
	}
	if agg.QuoteAssetVolume != quoteVolume {
		t.Errorf("agg QuoteAssetVolume: expected %.2f, got %.2f", quoteVolume, agg.QuoteAssetVolume)
	}
	if agg.Trades != trades {
		t.Errorf("agg Trades: expected %d, got %d", trades, agg.Trades)
	}
	if agg.CloseTime != startTime+60000-1 {
		t.Errorf("agg CloseTime: expected %d, got %d", startTime+60000-1, agg.CloseTime)
	}
	// DateTime is overwritten to time.Now() in saveAggregated, so just check it's nonzero
	if agg.DateTime == 0 {
		t.Error("agg DateTime should be nonzero (overwritten to time.Now())")
	}
}

// --- helpers ---

func buildKline(symbol string, startTime int64, price float64, isFinal bool) *futures.WsKline {
	return &futures.WsKline{
		StartTime:   startTime,
		EndTime:     startTime + 60000 - 1,
		Symbol:      symbol,
		Open:        fmt.Sprintf("%.2f", price),
		High:        fmt.Sprintf("%.2f", price+0.5),
		Low:         fmt.Sprintf("%.2f", price-0.5),
		Close:       fmt.Sprintf("%.2f", price+0.2),
		Volume:      "10.0",
		QuoteVolume: fmt.Sprintf("%.2f", 10.0*price),
		TradeNum:    50,
		IsFinal:     isFinal,
	}
}
