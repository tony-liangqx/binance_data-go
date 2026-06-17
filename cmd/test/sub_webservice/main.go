package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"

	"github.com/gorilla/websocket"
)

var ALL_SYMBOLS = []string{
	"ETHUSDT",
	"SOLUSDT",
	"TRXUSDT",
	"DOGEUSDT",
	"XRPUSDT",
	"LTCUSDT",
	"SUIUSDT",
	"ZKUSDT",
	"AAVEUSDT",
	"AVAXUSDT",
	"ZECUSDT",
	"1000PEPEUSDT",
	"OPUSDT",
	"ADAUSDT",
	"LINKUSDT",
	"UNIUSDT",
	"TONUSDT",
}

func main() {
	server := flag.String("server", "ws://localhost:8080", "WebSocket server URL")
	ratio := flag.String("period", "1m", "volatility level")
	flag.Parse()
	if *ratio != "1m" {
		log.Println("invalid period, must be 1m")
		os.Exit(1)
	}
	suffix := fmt.Sprintf("@volatility_%s", *ratio)
	streams := "BTCUSDT" + suffix
	for _, symbol := range ALL_SYMBOLS {
		streams += "/" + symbol + suffix
	}

	u, err := url.Parse(*server + "/stream?streams=" + streams)
	if err != nil {
		log.Printf("failed to parse URL: %v\n", err)
		os.Exit(1)
	}

	log.Printf("connecting to %s\n", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Printf("dial error: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	log.Println("connected, waiting for messages...")
	log.Println("press Ctrl+C to exit")

	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Printf("read error: %v\n", err)
				return
			}
			log.Println(string(message))
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	log.Println("\ninterrupt received, closing connection...")
	if err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
		log.Printf("write close error: %v\n", err)
	}
	<-done
	log.Println("connection closed")
}
