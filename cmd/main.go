package main

import (
	"fmt"

	"binance.data.sync/src/helper"
	"binance.data.sync/src/task"
)

func main() {
	fmt.Println("binance kline data sync starting...")

	// Get database storage (shared by all subscribers)
	storage := helper.GetStorage()
	defer func() {
		fmt.Println("binance kline data sync stopped")
	}()

	// Read all subscriptions from config
	subscriptions := helper.GetSubscriptions()
	if len(subscriptions) == 0 {
		fmt.Println("no subscriptions configured, exiting")
		return
	}

	fmt.Printf("loaded %d subscription(s):\n", len(subscriptions))

	// Create PubSubService for MQTT aggregation and publishing
	pubSubService := task.NewPubSubService()
	pubSubService.SetStorage(storage)
	go pubSubService.Start()

	// Create WebSocketService for streaming data to WebSocket clients
	wsService := task.NewWebSocketService(pubSubService)
	go wsService.Start()

	// Create and start a Subscriber for each symbol/period pair
	for _, sub := range subscriptions {
		fmt.Printf("  symbol=%s, period=%s\n", sub.Symbol, sub.Period)

		// Get the last saved timestamp to pass to the subscriber
		lastTime, err := storage.GetLastTimeStamp(sub.Symbol, sub.Period)
		if err != nil {
			fmt.Printf("failed to get last timestamp for %s/%s: %v, starting from 0\n",
				sub.Symbol, sub.Period, err)
			lastTime = 0
		}
		fmt.Printf("[%s/%s] last saved timestamp: %d\n", sub.Symbol, sub.Period, lastTime)

		// Create and start the Subscriber in a goroutine
		subscriber := task.NewSubscriber(storage, sub.Symbol, sub.Period)

		// Wire up PubSubService to receive processed points from this subscriber
		// 源数据传递到推送服务
		subscriber.SetPointChan(pubSubService.PointChan)

		go subscriber.Start(lastTime)
	}

	fmt.Println("all subscribers started, waiting for kline data...")

	// Block the main goroutine indefinitely
	select {}
}
