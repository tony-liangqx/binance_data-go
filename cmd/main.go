package main

import (
	"fmt"

	"binance.data.sync/src/helper"
	"binance.data.sync/src/task"
)

func main() {
	fmt.Println("binance kline data sync starting...")

	// Get database storage
	storage := helper.GetStorage()
	defer func() {
		fmt.Println("binance kline data sync stopped")
	}()

	// Read symbol and period from config
	symbol := helper.GetSymbol()
	period := helper.GetPeriod()

	fmt.Printf("config: symbol=%s, period=%s\n", symbol, period)

	// Get the last saved timestamp to pass to the subscriber
	lastTime, err := storage.GetLastTimeStamp(symbol, period)
	if err != nil {
		fmt.Printf("failed to get last timestamp: %v, starting from 0\n", err)
		lastTime = 0
	}
	fmt.Printf("last saved timestamp: %d\n", lastTime)

	// Create and start the Subscriber in a goroutine
	subscriber := task.NewSubscriber(storage, symbol, period)
	go subscriber.Start(lastTime)

	fmt.Println("subscriber started, waiting for kline data...")

	// Block the main goroutine indefinitely
	select {}
}
