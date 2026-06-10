package task

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"binance.data.sync/src/model"

	"github.com/gorilla/websocket"
)

const (
	// pingInterval is how often the server sends PING to connected clients
	pingInterval = 20 * time.Second

	// pongWait is how long the server waits for a PONG response before closing the connection
	pongWait = 30 * time.Second

	// writeWait is the time allowed to write a message to the client
	writeWait = 10 * time.Second

	// wsServerAddr is the default WebSocket server address
	wsServerAddr = ":8081"
)

// upgrader upgrades HTTP connections to WebSocket
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for simplicity
	},
}

// wsClient represents a single WebSocket client connection.
type wsClient struct {
	conn    *websocket.Conn
	streams []string // The stream names this client subscribed to
	send    chan []byte
}

// WebSocketService is a WebSocket server that relays kline data from PubSubService
// to connected WebSocket clients. It supports multi-stream subscriptions
// via the /stream?streams=<name1>/<name2>/... URL format.
//
// Unlike the previous implementation that relied on MQTT as middleware, this
// version subscribes directly to PubSubService via Go channels, eliminating
// the external MQTT dependency for internal data distribution.
type WebSocketService struct {
	pubSrv *PubSubService
	addr   string

	// storage is used by REST API handlers (e.g., /fapi/v1/klines)
	// to query historical kline data from the database.
	storage model.Storage
}

// NewWebSocketService creates a new WebSocketService with default settings.
func NewWebSocketService(pubSrv *PubSubService) *WebSocketService {
	return &WebSocketService{
		pubSrv: pubSrv,
		addr:   wsServerAddr,
	}
}

// NewWebSocketServiceWithBroker creates a new WebSocketService with custom server address.
// The broker parameter is retained for API compatibility but is ignored since
// PubSubService handles topic subscription internally via Go channels.
func NewWebSocketServiceWithBroker(pubSrv *PubSubService, broker, addr string) *WebSocketService {
	return &WebSocketService{
		pubSrv: pubSrv,
		addr:   addr,
	}
}

// SetStorage sets the storage backend used by REST API handlers.
func (s *WebSocketService) SetStorage(storage model.Storage) {
	s.storage = storage
}

// Start begins the WebSocket server.
func (s *WebSocketService) Start() {
	s.start()
}

// start starts the HTTP/WS server.
func (s *WebSocketService) start() {
	// Register WebSocket handler
	http.HandleFunc("/stream", s.handleStream)

	// Register REST API handlers
	if s.storage != nil {
		http.Handle("/fapi/v1/klines", &klinesHandler{storage: s.storage})
		fmt.Printf("[http-server] registered REST API: /fapi/v1/klines\n")

		http.Handle("/fapi/v1/volatility", &volatilityHandler{storage: s.storage})
		fmt.Printf("[http-server] registered REST API: /fapi/v1/volatility\n")

		http.Handle("/fapi/v1/volatility/all", &allVolatilityPointsHandler{storage: s.storage})
		fmt.Printf("[http-server] registered REST API: /fapi/v1/volatility/all\n")
	}

	fmt.Printf("[ws-server] starting WebSocket server on %s\n", s.addr)
	if err := http.ListenAndServe(s.addr, nil); err != nil {
		fmt.Printf("[ws-server] failed to start server: %v\n", err)
		panic(err)
	}
}

// handleStream handles the /stream WebSocket endpoint.
// URL format: /stream?streams=<streamName1>/<streamName2>/<streamName3>
func (s *WebSocketService) handleStream(w http.ResponseWriter, r *http.Request) {
	// Parse stream names from query parameter
	streamsParam := r.URL.Query().Get("streams")
	if streamsParam == "" {
		http.Error(w, "missing 'streams' query parameter", http.StatusBadRequest)
		return
	}

	streamNames := strings.Split(streamsParam, "/")
	if len(streamNames) == 0 {
		http.Error(w, "no stream names provided", http.StatusBadRequest)
		return
	}

	fmt.Printf("[ws-server] new connection request: streams=%v\n", streamNames)

	// 检查参数正确性
	symbols := make([]string, 0, len(streamNames))
	for _, streamName := range streamNames {
		// Parse symbol and period, and create aggregator for periods
		symbol, kind, period, ok := parseStreamName(streamName)
		if !ok {
			fmt.Printf("debug: parseStreamName error: %s\n", streamName)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		symbols = append(symbols, symbol)
		switch kind {
		case "volatility":
			switch period {
			case "10", "20", "30", "5":
				continue
			default:
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		case "kline":
		default:
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	// Upgrade HTTP to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("[ws-server] upgrade failed: %v\n", err)
		return
	}

	// 用户链接
	client := &wsClient{
		conn:    conn,
		streams: streamNames,
		send:    make(chan []byte, 256),
	}

	// Subscribe to topics for each stream and register the client directly
	// with PubSubService via Go channels (no MQTT middleware).
	for _, streamName := range streamNames {
		topic := streamNameToTopic(streamName)
		s.pubSrv.SubscribeLocal(topic, client.send)

		// Parse symbol and period, and create aggregator for periods
		// 如果是volatility类型，period为5、10、20等
		symbol, kind, period, ok := parseStreamName(streamName)
		if !ok {
			fmt.Printf("debug: parseStreamName error: %s\n", streamName)
			continue
		}
		point := s.pubSrv.Subscribe(symbol, kind, period)

		// 发送缓存message
		buf, err := json.Marshal(point)
		if err != nil {
			fmt.Printf("GetLatestPoint Marshal error:%s\n", err.Error())
			continue
		}
		if len(buf) == 0 {
			continue
		}
		client.send <- buf
	}

	// Start read and write goroutines for this client
	go s.writePump(client)
	go s.readPump(client)
}

// streamNameToTopic converts a stream name to an internal topic string.
// Example: "btcusdt@kline_1m" -> "binance/aggregated/btcusdt/1m"
// Example: "btcusdt@volatility_10" -> "binance/volatility/btcusdt/10"
//
// The topic string is used for both MQTT publishing (in PubSubService) and
// local in-process subscriptions.
func streamNameToTopic(streamName string) string {
	// Parse "symbol@kind_period" format
	parts := strings.SplitN(streamName, "@", 2)
	if len(parts) < 2 {
		// Fallback: use stream name as-is under the prefix
		return fmt.Sprintf("%s/%s", mqttTopicPrefix, streamName)
	}

	symbol := strings.ToUpper(parts[0])
	rest := parts[1]

	// Handle "kline_<period>" format
	if strings.HasPrefix(rest, "kline_") {
		period := strings.TrimPrefix(rest, "kline_")
		return fmt.Sprintf("%s/%s/%s", mqttTopicPrefix, symbol, period)
	}

	// Handle "volatility_<period>" format
	if strings.HasPrefix(rest, "volatility_") {
		volatility := strings.TrimPrefix(rest, "volatility_")
		return fmt.Sprintf("%s/%s/%s", mqttVolatilityTopicPrefix, symbol, volatility)
	}

	// Fallback
	return fmt.Sprintf("%s/%s/%s", mqttTopicPrefix, symbol, rest)
}

// parseStreamName extracts symbol and period from a stream name.
// Format: "btcusdt@kline_5m"      -> ("BTCUSDT", "kline", "5m", true)
// Format: "btcusdt@volatility_10" -> ("BTCUSDT", "volatility", "10", true)
// Returns ("", "", "", false) if the stream name cannot be parsed.
func parseStreamName(streamName string) (symbol, kind, period string, ok bool) {
	parts := strings.SplitN(streamName, "@", 2)
	if len(parts) != 2 {
		return "", "", "", false
	}
	symbol = strings.ToUpper(parts[0])
	rest := parts[1]
	parts = strings.SplitN(rest, "_", 2)
	if len(parts) != 2 {
		return "", "", "", false
	}
	kind = parts[0]
	period = parts[1]
	return symbol, kind, period, true
}

// writePump writes messages from the send channel to the WebSocket connection.
// It also sends PING messages every pingInterval.
func (s *WebSocketService) writePump(client *wsClient) {
	ticker := time.NewTicker(pingInterval)
	defer func() {
		ticker.Stop()
		client.conn.Close()
		// Clean up local subscriptions and aggregator references
		for _, streamName := range client.streams {
			topic := streamNameToTopic(streamName)
			s.pubSrv.UnsubscribeLocal(topic, client.send)

			// Unsubscribe aggregator for non-1m periods
			if symbol, kind, period, ok := parseStreamName(streamName); ok {
				s.pubSrv.Unsubscribe(symbol, kind, period)
			}
		}
	}()

	for {
		select {
		case message, ok := <-client.send:
			if !ok {
				// Channel was closed
				client.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			client.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := client.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				fmt.Printf("[ws-server] write error: %v\n", err)
				return
			}

		case <-ticker.C:
			// Send PING message with a unique payload (current timestamp)
			pingPayload := fmt.Sprintf(`{"ping":%d}`, time.Now().UnixMilli())
			client.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := client.conn.WriteMessage(websocket.PingMessage, []byte(pingPayload)); err != nil {
				fmt.Printf("[ws-server] ping error: %v\n", err)
				return
			}
		}
	}
}

// readPump reads messages from the WebSocket connection.
// It handles PONG messages and detects client disconnection.
func (s *WebSocketService) readPump(client *wsClient) {
	defer func() {
		client.conn.Close()
	}()

	client.conn.SetReadDeadline(time.Now().Add(pongWait))
	client.conn.SetPongHandler(func(appData string) error {
		fmt.Printf("[ws-server] received PONG: %s\n", appData)
		client.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	client.conn.SetPingHandler(func(appData string) error {
		// Respond to server-side ping with pong (should not normally happen from client)
		fmt.Printf("[ws-server] received unexpected PING from client: %s\n", appData)
		client.conn.SetReadDeadline(time.Now().Add(pongWait))
		return client.conn.WriteMessage(websocket.PongMessage, []byte(appData))
	})

	for {
		_, message, err := client.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				fmt.Printf("[ws-server] read error: %v\n", err)
			}
			// Close the send channel to stop writePump
			close(client.send)
			return
		}

		// Handle any text messages from the client (e.g., PONG responses)
		var msg map[string]interface{}
		if err := json.Unmarshal(message, &msg); err == nil {
			if _, ok := msg["pong"]; ok {
				fmt.Printf("[ws-server] received PONG message: %s\n", string(message))
			}
		}
	}
}
