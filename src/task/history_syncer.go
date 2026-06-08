package task

import (
	"context"
	"fmt"
	"time"

	"binance.data.sync/src/model"

	"github.com/adshao/go-binance/v2"
)

// TimestampProvider supplies the latest websocket timestamp to HistorySyncer,
// allowing it to dynamically determine how far it needs to backfill.
type TimestampProvider interface {
	GetTimeStamp() int64
	SyncDone()
}

// HistorySyncer uses Binance REST API to backfill historical kline data
// when a gap is detected between the last saved kline and the incoming websocket kline.
// It runs asynchronously and signals the Subscriber when it catches up.
type HistorySyncer struct {
	timeStamp     int64
	storage       model.Storage
	symbol        string
	period        string
	tsProvider    TimestampProvider
	savePointFunc func(point *model.FutureKlinePoint) error
}

// NewHistorySyncer creates a new HistorySyncer instance.
// tsProvider gives the syncer access to the Subscriber's latest websocket position.
func NewHistorySyncer(storage model.Storage, symbol string, period string, lastTime int64, tsProvider TimestampProvider, savePointFunc func(point *model.FutureKlinePoint) error) *HistorySyncer {
	return &HistorySyncer{
		storage:       storage,
		symbol:        symbol,
		period:        period,
		timeStamp:     lastTime,
		tsProvider:    tsProvider,
		savePointFunc: savePointFunc,
	}
}

// GetTimeStamp returns the last processed timestamp
func (h *HistorySyncer) GetTimeStamp() int64 {
	return h.timeStamp
}

// Start implements the Task interface
func (h *HistorySyncer) Start(timeStamp int64) {
	h.timeStamp = timeStamp
}

// Sync fetches historical klines from Binance REST API starting from the last saved record.
// It dynamically reads the Subscriber's latest websocket timestamp via tsProvider
// and keeps syncing until the backfilled data catches up. When done, it calls
// subscriber.SyncDone() to hand write control back to the Subscriber.
func (h *HistorySyncer) Sync() {
	// Ensure we signal completion when we exit
	defer func() {
		h.tsProvider.SyncDone()
	}()

	client := binance.NewClient("", "")
	fmt.Printf("[history] starting history sync: symbol=%s, period=%s, last_saved=%d\n",
		h.symbol, h.period, h.timeStamp)

	// 比数据库最后一条时间大
	currentStart := h.timeStamp + 1
	batchSize := 1000

	for {
		// If the subscriber hasn't advanced past our last saved point,
		// wait for more websocket data
		targetTime := h.tsProvider.GetTimeStamp()
		if currentStart > targetTime {
			fmt.Printf("[history] caught up to subscriber timestamp %d\n", targetTime)
			break
		}

		// 包括EndTime
		// Subscriber记录的最新一条记录是通过REST API获取到并且写入数据库
		klines, err := client.NewKlinesService().
			Symbol(h.symbol).
			Interval(h.period).
			StartTime(currentStart).
			EndTime(targetTime).
			Limit(batchSize).
			Do(context.TODO())
		if err != nil {
			fmt.Printf("[history] failed to fetch klines: %v\n", err)
			time.Sleep(time.Second)
			continue
		}

		if len(klines) == 0 {
			// No klines returned — check if we're truly caught up
			if currentStart > targetTime {
				break
			}
			time.Sleep(time.Second)
			continue
		}

		for _, k := range klines {
			point := &model.FutureKlinePoint{
				Symbol:                   h.symbol,
				Period:                   h.period,
				StartTime:                k.OpenTime,
				DateTime:                 k.OpenTime,
				Open:                     mustParseFloat(k.Open),
				High:                     mustParseFloat(k.High),
				Low:                      mustParseFloat(k.Low),
				Close:                    mustParseFloat(k.Close),
				Volume:                   mustParseFloat(k.Volume),
				CloseTime:                k.CloseTime,
				QuoteAssetVolume:         mustParseFloat(k.QuoteAssetVolume),
				Trades:                   uint32(k.TradeNum),
				TakerBuyBaseAssetVolume:  mustParseFloat(k.TakerBuyBaseAssetVolume),
				TakerBuyQuoteAssetVolume: mustParseFloat(k.TakerBuyQuoteAssetVolume),
			}

			h.savePointFunc(point)
		}

		// Check if we've caught up to the latest subscriber timestamp
		lastKline := klines[len(klines)-1]
		if lastKline.OpenTime >= targetTime {
			fmt.Printf("[history] %s %s caught up to target time %d\n", h.symbol, h.period, targetTime)
			break
		}

		// Move to the next batch
		currentStart = klines[len(klines)-1].OpenTime + 1

		// Rate limiting: avoid hitting Binance rate limits
		time.Sleep(200 * time.Millisecond)
	}
}

// Ensure compile-time interface compliance
var _ Task = (*HistorySyncer)(nil)
