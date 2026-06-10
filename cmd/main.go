package main

import (
	"log"

	"binance.data.sync/src/helper"
	"binance.data.sync/src/task"
)

func main() {
	log.Println("binance kline data sync starting...")

	// Get database storage (shared by all subscribers)
	storage := helper.GetStorage()
	defer func() {
		log.Println("binance kline data sync stopped")
	}()

	// Read all subscriptions from config
	subscriptions := helper.GetSubscriptions()
	if len(subscriptions) == 0 {
		log.Println("no subscriptions configured, exiting")
		return
	}

	log.Printf("loaded %d subscription(s):\n", len(subscriptions))

	// TODO: PubSubService 需要获得Subscriber的通知事件
	// Create PubSubService for MQTT aggregation and publishing
	pubSubService := task.NewPubSubService(len(subscriptions))
	pubSubService.SetStorage(storage)
	go pubSubService.Start()

	// Create WebSocketService for streaming data to WebSocket clients
	wsService := task.NewWebSocketService(pubSubService)
	wsService.SetStorage(storage)
	go wsService.Start()

	// Create a shared KlineConnection that all subscribers will use.
	// This ensures a single WebSocket connection handles all symbols,
	// routing incoming kline events to the correct subscriber by symbol.
	klineConn := task.NewKlineConnection()

	// Create and start a Subscriber for each symbol/period pair
	for _, sub := range subscriptions {
		log.Printf("  symbol=%s, period=%s\n", sub.Symbol, sub.Period)

		// Get the last saved timestamp to pass to the subscriber
		lastTime, err := storage.GetLastTimeStamp(sub.Symbol, sub.Period)
		if err != nil {
			log.Printf("failed to get last timestamp for %s/%s: %v, starting from 0\n",
				sub.Symbol, sub.Period, err)
			lastTime = 0
		}
		log.Printf("[%s/%s] last saved timestamp: %d\n", sub.Symbol, sub.Period, lastTime)

		// Create and start the Subscriber in a goroutine
		subscriber := task.NewSubscriber(storage, sub.Symbol, sub.Period)

		// Wire up the shared KlineConnection so all subscribers share
		// a single WebSocket connection instead of one per subscriber.
		subscriber.SetKlineConnection(klineConn)

		// Wire up PubSubService to receive processed points from this subscriber
		// 源数据传递到推送服务
		subscriber.SetPointChan(pubSubService.PointChan)
		subscriber.SetEventChan(pubSubService.AlignEventC, pubSubService.LoadHistoryEventC)

		go subscriber.Start(lastTime)
	}

	log.Println("all subscribers started, waiting for kline data...")

	// Block the main goroutine indefinitely
	select {}
}
