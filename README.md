# binance_data-go

# 流程图
![说明文字](./docs/流程图.png)

# 订阅服务`Data Flow`
```
Binance WS → Subscriber → DB（Symbol的`1m`数据）
                       ↓
                       → PubSubService (ch) → aggregate points → MQTT (mosquitto)
                                                                   ↓
                                                          WebSocketService (MQTT sub)
                                                                   ↓
                                                               WS Clients
```

## 历史数据部分
历史数据请求分：
1. 聚合后的历史
2. 聚合后的历史 + 指标

历史（基于1m的Kline数据）聚合、指标计算都由数据库完成。

## 实时数据部分
1. 最新数据（symbol，1m）缓存于内存
2. 聚合数据（symbol, period）的"订阅"都会创建`symbolAggregator`对象，聚合算法由此对象负责。
3. 聚合数据（symbol, period）的"指标"创建时需要“冷启动”数据集（触发数据库请求）,随后的滑动计算和结果存放由此对象负责

## 聚合数据
`symbolAggregator`为聚合数据（symbol, period）对象，记录了“上一个数据点”和需要的“指标”引用（数组）。

`Indicator`为指标算法对象，负责历史数据获取和滑动计算、结果存放。

## 用户订阅请求过程
当用户发起web socket订阅请求时（比如，"binance/aggregated/btcusdt/5m"），WebSocketService根据请求的创建
（symbol, period）为索引的`symbolAggregator`对象，并且创建默认的`Indicator`算法集。
