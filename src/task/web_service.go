package task

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"binance.data.sync/src/model"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gorilla/websocket"
)

const (
	// Default MQTT broker address for the WebSocket service
	wsDefaultMQTTBroker = "tcp://127.0.0.1:1883"

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

// topicSubscription manages MQTT topic subscriptions with reference counting.
type topicSubscription struct {
	refCount int
}

// WebSocketService is a WebSocket server that relays kline data from MQTT
// to connected WebSocket clients. It supports multi-stream subscriptions
// via the /stream?streams=<name1>/<name2>/... URL format.
type WebSocketService struct {
	pubSrv   *PubSubService
	broker   string
	addr     string
	mqttOpts *mqtt.ClientOptions

	// mqttClient is the shared MQTT client for receiving data
	mqttClient mqtt.Client

	// clients maps topic -> set of wsClient channels subscribed to that topic
	clients map[string]map[chan []byte]struct{}
	mu      sync.RWMutex

	// topics tracks MQTT topic subscriptions with reference counts
	topics map[string]*topicSubscription
}

// NewWebSocketService creates a new WebSocketService with default settings.
func NewWebSocketService(pubSrv *PubSubService) *WebSocketService {
	return NewWebSocketServiceWithBroker(pubSrv, wsDefaultMQTTBroker, wsServerAddr)
}

// NewWebSocketServiceWithBroker creates a new WebSocketService with custom MQTT broker and server address.
func NewWebSocketServiceWithBroker(pubSrv *PubSubService, broker, addr string) *WebSocketService {
	clientID := fmt.Sprintf("binance_ws_%d", time.Now().UnixNano())

	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(clientID)
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(true)
	opts.SetConnectTimeout(10 * time.Second)
	opts.SetKeepAlive(30 * time.Second)
	opts.SetOrderMatters(false)

	return &WebSocketService{
		pubSrv:   pubSrv,
		broker:   broker,
		addr:     addr,
		mqttOpts: opts,
		clients:  make(map[string]map[chan []byte]struct{}),
		topics:   make(map[string]*topicSubscription),
	}
}

// Start begins the WebSocket server.
func (s *WebSocketService) Start() {
	s.start()
}

// start connects to MQTT and starts the HTTP/WS server.
func (s *WebSocketService) start() {
	// Connect to MQTT broker
	client := mqtt.NewClient(s.mqttOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Printf("[ws-server] failed to connect to MQTT broker %s: %v\n", s.broker, token.Error())
		panic(token.Error())
	}
	s.mqttClient = client
	fmt.Printf("[ws-server] connected to MQTT broker %s (client_id=%s)\n", s.broker, s.mqttOpts.ClientID)

	// Register HTTP handler
	http.HandleFunc("/stream", s.handleStream)

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

	// Subscribe to MQTT topics for each stream and register the client
	for _, streamName := range streamNames {
		topic := s.streamNameToTopic(streamName)
		s.subscribeTopic(topic, client.send)

		// Parse symbol and period, and create aggregator for periods
		symbol, period, ok := parseStreamName(streamName)
		if !ok {
			fmt.Printf("debug: parseStreamName error: %s\n", streamName)
			continue
		}
		s.pubSrv.Subscribe(symbol, period)

		// 发送缓存message
		tokens := strings.Split(topic, "/")
		if len(tokens) != 4 {
			continue
		}
		// 获取内存中的聚合数据
		var point model.SpotKlinePoint
		s.mu.RLock()
		agg, ok := s.pubSrv.aggregators[tokens[2]+":"+tokens[3]]
		if ok && agg.FirstPoint() != nil {
			point = *agg.FirstPoint()
		}
		s.mu.RUnlock()

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

// streamNameToTopic converts a stream name to an MQTT topic.
// Example: "btcusdt@kline_1m" -> "binance/aggregated/btcusdt/1m"
func (s *WebSocketService) streamNameToTopic(streamName string) string {
	// Parse "symbol@kline_period" format
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

	// Fallback
	return fmt.Sprintf("%s/%s/%s", mqttTopicPrefix, symbol, rest)
}

// parseStreamName extracts symbol and period from a stream name.
// Format: "btcusdt@kline_5m" -> ("BTCUSDT", "5m", true)
// Returns ("", "", false) if the stream name cannot be parsed.
func parseStreamName(streamName string) (symbol, period string, ok bool) {
	parts := strings.SplitN(streamName, "@", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	symbol = strings.ToUpper(parts[0])
	rest := parts[1]
	if strings.HasPrefix(rest, "kline_") {
		period = strings.TrimPrefix(rest, "kline_")
	} else {
		period = rest
	}
	return symbol, period, true
}

// subscribeTopic subscribes to an MQTT topic and registers the client's send channel.
func (s *WebSocketService) subscribeTopic(topic string, sendChan chan []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Register client channel for this topic
	if _, ok := s.clients[topic]; !ok {
		s.clients[topic] = make(map[chan []byte]struct{})
	}
	s.clients[topic][sendChan] = struct{}{}

	// Subscribe to MQTT topic if not already subscribed
	if _, ok := s.topics[topic]; !ok {
		s.topics[topic] = &topicSubscription{refCount: 0}
	}

	sub := s.topics[topic]
	if sub.refCount == 0 {
		// First subscription to this topic
		token := s.mqttClient.Subscribe(topic, 1, func(client mqtt.Client, msg mqtt.Message) {
			s.dispatchMessage(msg.Topic(), msg.Payload())
		})
		token.Wait()
		if token.Error() != nil {
			fmt.Printf("[ws-server] failed to subscribe to MQTT topic %s: %v\n", topic, token.Error())
		} else {
			fmt.Printf("[ws-server] subscribed to MQTT topic: %s\n", topic)
		}
	}
	sub.refCount++

	fmt.Printf("[ws-server] client subscribed to topic %s (refCount=%d)\n", topic, sub.refCount)
}

// unsubscribeTopic unsubscribes from an MQTT topic and removes the client's send channel.
func (s *WebSocketService) unsubscribeTopic(topic string, sendChan chan []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove client channel from this topic
	if clients, ok := s.clients[topic]; ok {
		delete(clients, sendChan)
		if len(clients) == 0 {
			delete(s.clients, topic)
		}
	}

	// Decrement reference count and unsubscribe if no more clients
	if sub, ok := s.topics[topic]; ok {
		sub.refCount--
		if sub.refCount <= 0 {
			token := s.mqttClient.Unsubscribe(topic)
			token.Wait()
			if token.Error() != nil {
				fmt.Printf("[ws-server] failed to unsubscribe from MQTT topic %s: %v\n", topic, token.Error())
			} else {
				fmt.Printf("[ws-server] unsubscribed from MQTT topic: %s\n", topic)
			}
			delete(s.topics, topic)
		}
	}
}

// dispatchMessage sends an MQTT message payload to all WebSocket clients
// subscribed to the given topic.
func (s *WebSocketService) dispatchMessage(topic string, payload []byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if clients, ok := s.clients[topic]; ok {
		for sendChan := range clients {
			select {
			case sendChan <- payload:
			default:
				// Client channel full, skip
			}
		}
	}
}

// writePump writes messages from the send channel to the WebSocket connection.
// It also sends PING messages every pingInterval.
func (s *WebSocketService) writePump(client *wsClient) {
	ticker := time.NewTicker(pingInterval)
	defer func() {
		ticker.Stop()
		client.conn.Close()
		// Clean up MQTT topic subscriptions and aggregator references
		for _, streamName := range client.streams {
			topic := s.streamNameToTopic(streamName)
			s.unsubscribeTopic(topic, client.send)

			// Unsubscribe aggregator for non-1m periods
			if symbol, period, ok := parseStreamName(streamName); ok && period != "1m" {
				s.pubSrv.Unsubscribe(symbol, period)
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
		// Close the send channel to signal writePump to clean up
		// Note: we don't close it here because writePump uses it.
		// Instead, we wait for the connection to close naturally.
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
