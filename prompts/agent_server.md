基于src/task/start_service.go代码，实现一个websocket的服务，要求：
1. 请求的URL参数为stream，支持组合streams，URL格式为 `/stream?streams=<streamName1>/<streamName2>/<streamName3>`
2. WebSocket 服务器每20秒发送 PING 消息，当客户收到PING消息，必须尽快回复PONG消息，同时payload需要和PING消息一致。
3. 背后是mosquitto中间件


基于src/task/pub.go代码，实现一个推送服务，要求：
1. 对src/task/subscribe.go 中的Subscriber.processPoint数据进行聚合
2. 聚合算法是10个点聚合成一个点（源数据格式为BinanceSpotKline）
3. 把聚合后的数据推送到mosquitto中间件
