# binance_data-go

# 流程图
![说明文字](./docs/流程图.png)

# 订阅服务`Data Flow`
```
Binance WS → Subscriber → DB (existing)
                        ↓
                        → PubSubService (ch) → aggregate points → MQTT (mosquitto)
                                                                 ↓
                                                                 → WebSocketService (MQTT sub) → WS Clients
```
