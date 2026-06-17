把当前go项目用Rust语言实现，要求：
1. 新项目的路径是/Users/liangqingxi/sources/rust/binance_data-rust
2. PubSubService.AlignEventC使用Rust的mpsc替代
3. PubSubService.LoadHistoryEventC使用Rust的tokio::sync::Notify替代
4. Subscriber.aggregators在一个循环中就添加完成，尽量减少锁的使用。
5. ClickHouse 存储实现: 实现 `Storage` trait，使用 `clickhouse` crate 替代 GORM
6. Binance WebSocket 连接: 使用 `tokio-tungstenite` 实现 `KlineConnection.run()`
7. Binance REST API 客户端: 使用 `reqwest` 实现 `HistorySyncer.sync()` 的历史数据回填
8. WebSocket 服务: 使用 `tokio-tungstenite` 实现完整的 `/stream` 端点
9. K 线聚合 SQL: 从 Go 的 GORM/ClickHouse SQL 迁移聚合查询（`buildBucketExpr` 等）
