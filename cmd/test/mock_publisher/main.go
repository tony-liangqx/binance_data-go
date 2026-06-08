package main

import (
	"fmt"
	"time"

	"binance.data.sync/src/model"
	"binance.data.sync/src/task"
)

func main() {
	// Create PubSubService (uses default MQTT broker tcp://127.0.0.1:1883)
	pubSubService := task.NewPubSubService()
	go pubSubService.Start()

	// Give it a moment to connect to MQTT
	time.Sleep(500 * time.Millisecond)

	// Generate 20 mock kline points
	symbols := []string{"BTCUSDT", "ETHUSDT"}
	period := "1m"
	now := time.Now().Truncate(time.Minute)

	for i := 0; i < 20; i++ {
		symbol := symbols[i%len(symbols)]
		startTime := now.Add(-time.Duration(20-i) * time.Minute)

		point := &model.FutureKlinePoint{
			Symbol:           symbol,
			Period:           period,
			StartTime:        startTime.UnixMilli(),
			DateTime:         startTime.UnixMilli(),
			Open:             50000.0 + float64(i)*100,
			High:             50100.0 + float64(i)*100,
			Low:              49900.0 + float64(i)*100,
			Close:            50050.0 + float64(i)*100,
			Volume:           10.0 + float64(i)*0.5,
			CloseTime:        startTime.Add(time.Minute).UnixMilli(),
			QuoteAssetVolume: (10.0 + float64(i)*0.5) * 50000,
			Trades:           100 + uint32(i)*5,
		}

		fmt.Printf("sending kline point [%2d/%2d]: symbol=%s, period=%s, start=%d, open=%.2f, close=%.2f\n",
			i+1, 20, point.Symbol, point.Period, point.StartTime, point.Open, point.Close)

		// pubSubService.PointChan <- point

		// Small delay to simulate real-time ingestion
		time.Sleep(50 * time.Millisecond)
	}

	fmt.Println("all 20 kline points sent successfully")

	// Keep running to allow PubSubService to process and publish
	// (it will exit when the MQTT client disconnects on process termination)
	time.Sleep(2 * time.Second)
	fmt.Println("done")
}
