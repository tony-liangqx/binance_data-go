使用GORM框架
GORM的表结构struct是src/model/database.go中的BinanceSpotKline
进程由cmd/main.go中的main函数启动
数据库的链接通过src/helper/func.go中的GetStorage函数提供
获取数据库链接后，启动Subscriber协程

Subscriber结构体提供的功能：
1. 通过binance官方的go SDK，实现websocket订阅K线数据
2. 过滤订阅的K线数据只保留闭合的数据
3. 最新的闭合数据与上一个闭合数据进行比较，如果满足相差60秒，将过滤后的数据保存到数据库中
4. 如果不满足相差60秒，启动HistorySyncer协程
5. 启动HistorySyncer协程后，通过binance的REST API同步历史数据，将数据保存到数据库中，
6. HistorySyncer直到闭合数据与当前最新时间戳一致，退出协程
7. HistorySyncer退出后，Subscriber协程继续订阅新的K线数据


Subscriber与HistorySyncer的同步逻辑关系：
1. Subscriber启动HistorySyncer后是异步，不能阻塞websocket stream的数据处理
2. 启动HistorySyncer后，Subscriber会继续读取websocket stream的数据，但没有写入权限，直到HistorySyncer同步完成后才恢复
3. HistorySyncer通过Subscriber.GetTimeStamp方法获得websocket最新位置
4. HistorySyncer会一直同步，直到point.StartTime == Subscriber.GetTimeStamp方法返回的时间戳才结束
5. HistorySyncer同步完成后退出协程，Subscriber继续进行同步任务（通过Subscriber.SyncDone方法）


手动修复的功能：
1. src/helper/func.go 配置文件中是单个symbol的订阅，请修改成多订阅的模式， 要求：AppConfig struct中有一个数组，记录了每一个symbol和period信息。
2. main.go 在构造task.NewSubscriber时，每一对symbol和period有一个对应的task.NewSubscriber。
3. Subscriber.Start方法能处理网络重连
