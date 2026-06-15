package task

import (
	"log"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

// KlineConnection manages a single Binance WebSocket connection that
// subscribes to multiple kline streams and dispatches events to the
// appropriate Subscriber based on the symbol in the event data.
//
// All symbols use the same interval (default "1m"). The connection
// is re-established automatically on failure after a 10-second delay.
type KlineConnection struct {
	subscribers map[string]*Subscriber // symbol -> subscriber
	mu          sync.RWMutex
	started     bool
}

// NewKlineConnection creates a new KlineConnection.
func NewKlineConnection() *KlineConnection {
	return &KlineConnection{
		subscribers: make(map[string]*Subscriber),
	}
}

// Register adds a subscriber to this connection.
// The first call to Register starts the shared WebSocket connection.
// Subsequent calls add the subscriber to the routing table.
//
// On the next reconnection, the new symbol will be included automatically.
func (kc *KlineConnection) Register(sub *Subscriber) {
	kc.mu.Lock()
	kc.subscribers[sub.symbol] = sub
	if !kc.started {
		kc.started = true
		go kc.run()
	}
	kc.mu.Unlock()

	log.Printf("[shared_ws] subscriber registered: symbol=%s, total=%d\n",
		sub.symbol, len(kc.subscribers))
}

// buildSymbolMap returns a snapshot of the current symbol->interval map.
// Called inside run() to build the subscription list for WsCombinedKlineServe.
func (kc *KlineConnection) buildSymbolMap() map[string]string {
	kc.mu.RLock()
	defer kc.mu.RUnlock()

	symbols := make(map[string]string, len(kc.subscribers))
	for symbol, sub := range kc.subscribers {
		symbols[symbol] = sub.period
	}
	return symbols
}

// dispatchEvent is the WsKlineHandler callback for WsCombinedKlineServe.
// It looks up the subscriber by event.Symbol and forwards the event.
func (kc *KlineConnection) dispatchEvent(event *futures.WsKlineEvent) {
	kc.mu.RLock()
	sub, ok := kc.subscribers[event.Symbol]
	kc.mu.RUnlock()

	if !ok {
		log.Printf("[shared_ws] no subscriber for symbol: %s\n", event.Symbol)
		return
	}
	sub.handleKline(event)
}

// handleError is the ErrHandler callback for WsCombinedKlineServe.
func (kc *KlineConnection) handleError(err error) {
	log.Printf("[shared_ws] websocket error: %v\n", err)
}

// run starts the shared WebSocket connection and manages reconnection.
// It collects all registered symbols and uses WsCombinedKlineServe to
// subscribe to all of them in a single connection.
func (kc *KlineConnection) run() {
	for {
		symbols := kc.buildSymbolMap()

		if len(symbols) == 0 {
			log.Println("[shared_ws] no subscribers registered, waiting...")
			time.Sleep(10 * time.Second)
			continue
		}

		log.Printf("[shared_ws] connecting with %d symbols...\n", len(symbols))

		doneC, stopC, err := futures.WsCombinedKlineServe(symbols, kc.dispatchEvent, kc.handleError)
		if err != nil {
			log.Printf("[shared_ws] failed to start connection: %v\n", err)
			time.Sleep(10 * time.Second)
			continue
		}

		log.Printf("[shared_ws] connection established with %d symbols\n", len(symbols))

		<-doneC
		log.Println("[shared_ws] connection closed, reconnecting in 10s...")
		_ = stopC
		time.Sleep(10 * time.Second)
	}
}
