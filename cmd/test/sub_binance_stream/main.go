package main

import (
	"fmt"

	"github.com/adshao/go-binance/v2"
)

func main() {
	fmt.Println("starting websocket kline subscription for BNB/1m...")

	doneC, stopC, err := binance.WsKlineServe("BNBBTC", "1m", handleKline, handleError)
	if err != nil {
		fmt.Printf("failed to start kline websocket: %v\n", err)
		return
	}

	fmt.Println("websocket connected, waiting for kline data...")
	_ = stopC

	// Block until the connection is closed
	<-doneC
	fmt.Println("websocket connection closed")
}

// handleKline processes each incoming kline event
func handleKline(event *binance.WsKlineEvent) {
	kline := event.Kline
	fmt.Printf("kline: symbol=%s, start=%d, end=%d, interval=%s, open=%.2f, high=%.2f, low=%.2f, close=%.2f, volume=%.4f, isFinal=%v\n",
		kline.Symbol, kline.StartTime, kline.EndTime, kline.Interval,
		parseFloat(kline.Open), parseFloat(kline.High),
		parseFloat(kline.Low), parseFloat(kline.Close),
		parseFloat(kline.Volume), kline.IsFinal,
	)
}

// handleError handles websocket errors
func handleError(err error) {
	fmt.Printf("websocket error: %v\n", err)
}

// parseFloat converts a string to float64
func parseFloat(s string) float64 {
	var v float64
	_, err := fmt.Sscanf(s, "%f", &v)
	if err != nil {
		return 0
	}
	return v
}
