# binance_data-go

# 订阅服务`Data Flow`
```
Binance WS → Subscriber → DB（Symbol的`1m`数据）
                       ↓
                       → PubSubService → aggregate points → MQTT (mosquitto)
                                                                   ↓
                                                          WebSocketService (MQTT sub)
                                                                   ↓
                                                               WS Clients
```
`PubSubService`为数据管理中枢。

1. 内存缓存的数据用于实时“聚合”和计算“指标”。
2. MQTT用于多路订阅通道数据复制。

# 数据同步流程图
`Subscriber`负责源数据同步
![说明文字](./docs/流程图.png)


## 历史数据部分
历史数据请求分：
1. 聚合后的历史
2. 聚合后的历史 + 指标

历史（基于1m的Kline数据）聚合、指标计算都由数据库完成。

## 实时数据部分
1. 聚合数据（symbol, period）的"订阅"都会创建`symbolAggregator`对象，聚合算法由此对象负责。
2. 聚合数据（symbol, period）的"指标"创建时需要“冷启动”数据集（触发数据库请求）,随后的滑动计算和结果存放由此对象负责

不同的`Aggregator`类型有不同的聚合方式

不同的`Indicator`类型有不同的指标算法

已有的`Aggregator`类型：
- `symbolAggregator`：基于Kline数据的聚合
- `priceChangeAggregator`：基于价格变动的聚合

已有的`Indicator`类型：
- `SMAIndicator`：简单移动平均
- `VolumeDensityIndicator`：成交量密度

## 聚合数据过程
`symbolAggregator`为聚合数据（symbol, period）对象，记录了“上一个数据点”和需要的“指标”引用（数组）。

`Indicator`为指标算法对象，负责历史数据获取和滑动计算、结果存放。

每一个“数据点”历遍全部`symbolAggregator`。

聚合发生时历遍`symbolAggregator`配置的全部`Indicator`

## 用户订阅请求过程
当用户发起web socket订阅请求时（比如，"binance/aggregated/btcusdt/5m"），WebSocketService根据请求的创建
（symbol, period）为索引的`symbolAggregator`对象，并且创建默认的`Indicator`算法集。

# 数据样例
带有“指标”的数据格式
```
{
    "time": 1717462800,
    "open": 65200.5, "high": 65350.0, "low": 65180.0, "close": 65300.0, "volume": 120.5,
    "indicators": {
      "ma5": 65220.1,
      "volume_density": 1087.67
    }
  }
```


# 历史API
```
GET /fapi/v1/klines?symbol=<symbol>&interval=<period>&limit=<limit>

GET /fapi/v1/volatility?symbol=<symbol>&interval=<5\10\20>&limit=<limit>

// 完整volatility + 指标
GET /fapi/v1/volatility/all
```
例子：
```
GET /fapi/v1/klines?symbol=btcusdt&interval=15m&limit=100

GET /fapi/v1/volatility?symbol=btcusdt&interval=5&limit=100
```
