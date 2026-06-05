当前项目通过GORM使用mysql后端。请把当前项目改成clickhouse后端。要求规则：
1. 使用MergeTree引擎
2. 必须禁用事务：`SkipDefaultTransaction: true`
3. 修改src/model/database.go 里的 BinanceSpotKline 结构体，适配clickhouse
4. 适配K线数据存储，合理优化 BinanceSpotKline 结构体排序键
