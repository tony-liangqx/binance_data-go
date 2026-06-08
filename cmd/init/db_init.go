package main

import (
	"context"
	"fmt"
	"strconv"

	"binance.data.sync/src/helper"
	"binance.data.sync/src/model"

	"github.com/adshao/go-binance/v2/futures"
)

func main() {
	fmt.Println("initializing database tables...")

	// GetStorage creates the DB connection and auto-migrates BinanceSpotKline
	storage := helper.GetStorage()
	db := storage.GetDB()

	// Auto migrate the schema
	if err := db.
		AutoMigrate(&model.BinanceFutureKline{}); err != nil {
		panic(fmt.Errorf("failed to auto migrate: %w", err))
	}

	if err := db.Set("gorm:table_options", `
ENGINE = ReplacingMergeTree()
ORDER BY (symbol, period, volatility, start_time)
PRIMARY KEY (symbol, period, volatility, start_time)
`).AutoMigrate(&model.AggBinanceFutureKline{}); err != nil {
		panic(fmt.Errorf("failed to auto migrate: %w", err))
	}

	fmt.Println("database tables created successfully")

	// Read subscriptions (symbol + period pairs) from config.json
	subscriptions := helper.GetSubscriptions()
	if len(subscriptions) == 0 {
		fmt.Println("no subscriptions found in config.json")
		return
	}

	// Initialize data for each subscription using Binance Futures REST API
	client := futures.NewClient("", "")
	// Start time for the first record: 2026-01-01 00:00:00 UTC
	startTime := int64(1767974400000)

	for _, sub := range subscriptions {
		fmt.Printf("initializing data for symbol=%s, period=%s\n", sub.Symbol, sub.Period)

		// Create volatility data writers for aggregated kline table
		aggregators := []*model.VolatilityDataWriter{
			model.NewVolatilityDataWriter(sub.Symbol, 0.5, storage),
			model.NewVolatilityDataWriter(sub.Symbol, 1, storage),
			model.NewVolatilityDataWriter(sub.Symbol, 2, storage),
		}

		// Fetch historical klines from Binance Futures API
		klines, err := client.NewKlinesService().
			Symbol(sub.Symbol).
			Interval(sub.Period).
			StartTime(startTime).
			Limit(1).
			Do(context.TODO())
		if err != nil {
			fmt.Printf("failed to fetch klines for %s %s: %v\n", sub.Symbol, sub.Period, err)
			continue
		}

		fmt.Printf("fetched %d klines for %s %s\n", len(klines), sub.Symbol, sub.Period)

		for _, k := range klines {
			point := &model.FutureKlinePoint{
				Symbol:           sub.Symbol,
				Period:           sub.Period,
				StartTime:        k.OpenTime,
				DateTime:         k.OpenTime,
				Open:             mustParseFloat(k.Open),
				High:             mustParseFloat(k.High),
				Low:              mustParseFloat(k.Low),
				Close:            mustParseFloat(k.Close),
				Volume:           mustParseFloat(k.Volume),
				CloseTime:        k.CloseTime,
				QuoteAssetVolume: mustParseFloat(k.QuoteAssetVolume),
				Trades:           uint32(k.TradeNum),
				// Binance Futures REST kline has TakerBuyBaseAssetVolume /
				// TakerBuyQuoteAssetVolume fields (not ActiveBuyVolume/ActiveBuyQuoteVolume)
				TakerBuyBaseAssetVolume:  mustParseFloat(k.TakerBuyBaseAssetVolume),
				TakerBuyQuoteAssetVolume: mustParseFloat(k.TakerBuyQuoteAssetVolume),
			}

			// 1) Save to BinanceFutureKline table
			if err := storage.Commit(point); err != nil {
				fmt.Printf("failed to save kline: %v\n", err)
				continue
			}

			// 2) Process through volatility aggregators -> AggBinanceFutureKline table
			for _, aggregator := range aggregators {
				if _, err := aggregator.Add(point); err != nil {
					fmt.Printf("failed to aggregate point: %v\n", err)
				}
			}
		}

		fmt.Printf("initialization completed for symbol=%s, period=%s, total klines=%d\n",
			sub.Symbol, sub.Period, len(klines))
	}

	fmt.Println("database initialization completed")
}

// mustParseFloat parses a string to float64, defaults to 0 on error
func mustParseFloat(s string) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		fmt.Printf("warning: failed to parse float %q: %v\n", s, err)
		return 0
	}
	return v
}
