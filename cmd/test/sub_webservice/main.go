package main

import (
	"fmt"
	"net/url"
	"os"
	"os/signal"

	"github.com/gorilla/websocket"
)

func main() {
	server := "ws://localhost:8080"
	streams := "BTCUSDT@kline_1m/ETHUSDT@kline_1m"

	u, err := url.Parse(server + "/stream?streams=" + streams)
	if err != nil {
		fmt.Printf("failed to parse URL: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("connecting to %s\n", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		fmt.Printf("dial error: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	fmt.Println("connected, waiting for messages...")
	fmt.Println("press Ctrl+C to exit")

	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				fmt.Printf("read error: %v\n", err)
				return
			}
			fmt.Println(string(message))
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	fmt.Println("\ninterrupt received, closing connection...")
	if err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
		fmt.Printf("write close error: %v\n", err)
	}
	<-done
	fmt.Println("connection closed")
}
