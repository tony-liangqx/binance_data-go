package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"strconv"
)

// Config 配置参数
const (
	Threshold = 0.01
)

var logBase = math.Log(1 + Threshold)

// Kline 输入的原始K线数据（例如1m K线）
type Kline struct {
	Symbol        string  `json:"symbol"`
	OpenTime      int64   `json:"start_time"`
	CloseTime     int64   `json:"close_time"`
	Open          float64 `json:"open"`
	High          float64 `json:"high"`
	Low           float64 `json:"low"`
	Close         float64 `json:"close"`
	Volume        float64 `json:"volume"`
	QuoteVolume   float64 `json:"quote_volume"`
	Trades        int64   `json:"trades"`
	TakerBuyBase  float64 `json:"taker_buy_base"`
	TakerBuyQuote float64 `json:"taker_buy_quote"`
}

// GridKline 聚合后的网格K线
type GridKline struct {
	Symbol        string
	OpenTime      int64
	CloseTime     int64
	Open          float64
	High          float64
	Low           float64
	Close         float64
	Volume        float64
	QuoteVolume   float64
	Trades        int64
	TakerBuyBase  float64
	TakerBuyQuote float64
	Count         int64 // 包含原始K线的数量
	GridID        int64
}

// GridAggregator 状态机：每个交易对（Symbol）需要独立维护一个实例
type GridAggregator struct {
	symbol       string
	currentGrid  *GridKline // 当前正在累加的网格K线缓存
	lastActualID int64      // 对应 Python 中 grid_id_actual 的最新状态
	isFirstKline bool       // 是否是第一根K线
}

// NewGridAggregator 创建一个状态机实例
func NewGridAggregator(symbol string) *GridAggregator {
	return &GridAggregator{
		symbol:       symbol,
		isFirstKline: true,
	}
}

// calcGrid 计算价格对应的网格线（浮点）
func calcGrid(price float64) float64 {
	return math.Log(price) / logBase
}

// Feed 核心逻辑：每来一根K线，调用一次该函数。
// 如果该K线导致了网格破位切换，则会返回一个聚合完毕的 *GridKline，否则返回 nil。
func (a *GridAggregator) Feed(k Kline) *GridKline {
	if k.Symbol != a.symbol {
		return nil
	}

	openGrid := calcGrid(k.Open)
	closeGrid := calcGrid(k.Close)

	floorOpen := math.Floor(openGrid)
	floorClose := math.Floor(closeGrid)

	// 1. 初始化第一根 K 线
	if a.isFirstKline {
		// 计算初始的 grid_id_actual
		actualID := int64(0)
		if floorOpen != floorClose {
			if closeGrid > openGrid {
				actualID = int64(floorClose)
			} else {
				actualID = int64(floorClose) + 1
			}
		} else {
			// 如果第一根没穿越，暂取 floorClose 作为初始 ID 兜底 (对应 bfill 逻辑)
			actualID = int64(floorClose)
		}

		a.lastActualID = actualID
		a.isFirstKline = false

		// 开启第一个网格 Buffer
		// 注意：Python 中移位操作 `shift(1).bfill()` 导致初始 grid_id 等于第一根的 actualID
		a.currentGrid = &GridKline{
			Symbol:        k.Symbol,
			OpenTime:      k.OpenTime,
			CloseTime:     k.CloseTime,
			Open:          k.Open,
			High:          k.High,
			Low:           k.Low,
			Close:         k.Close,
			Volume:        k.Volume,
			QuoteVolume:   k.QuoteVolume,
			Trades:        k.Trades,
			TakerBuyBase:  k.TakerBuyBase,
			TakerBuyQuote: k.TakerBuyQuote,
			Count:         1,
			GridID:        actualID,
		}
		return nil
	}

	// 2. 计算当前 K 线的实际网格（对应 Python 向量化穿越与 ffill）
	currentActualID := a.lastActualID // 默认向前填充 (ffill)
	if floorOpen != floorClose {
		if closeGrid > openGrid {
			currentActualID = int64(floorClose)
		} else {
			currentActualID = int64(floorClose) + 1
		}
	}

	// 3. 判断是否触发网格切换
	// 严格对应 Python 逻辑：grid_id = actualID.shift(1)
	// 也就是说，当前的 actualID 变化了，不影响当前 K 线的分组（它依旧留在老组中结算）。
	// 只有当“上一次”的 actualID 与当前网格正在运行的 GridID 不同，才意味着“上一根”完成了触发，当前这根进入新组。
	var completedGrid *GridKline

	if a.lastActualID != a.currentGrid.GridID {
		// 触发切换：把当前的 Buffer 作为已完成的网格 K 线吐出
		completedGrid = a.currentGrid

		// 用当前这根 K 线创建全新的网格组，其 GridID 继承上一次的 actualID
		a.currentGrid = &GridKline{
			Symbol:        k.Symbol,
			OpenTime:      k.OpenTime,
			CloseTime:     k.CloseTime,
			Open:          k.Open,
			High:          k.High,
			Low:           k.Low,
			Close:         k.Close,
			Volume:        k.Volume,
			QuoteVolume:   k.QuoteVolume,
			Trades:        k.Trades,
			TakerBuyBase:  k.TakerBuyBase,
			TakerBuyQuote: k.TakerBuyQuote,
			Count:         1,
			GridID:        a.lastActualID, // 承接 shift(1) 后的 ID
		}
	} else {
		// 未触发切换：更新当前的 Buffer (累加 High, Low, Volume, 计数等)
		a.currentGrid.CloseTime = k.CloseTime
		a.currentGrid.Close = k.Close
		if k.High > a.currentGrid.High {
			a.currentGrid.High = k.High
		}
		if k.Low < a.currentGrid.Low {
			a.currentGrid.Low = k.Low
		}
		a.currentGrid.Volume += k.Volume
		a.currentGrid.QuoteVolume += k.QuoteVolume
		a.currentGrid.Trades += k.Trades
		a.currentGrid.TakerBuyBase += k.TakerBuyBase
		a.currentGrid.TakerBuyQuote += k.TakerBuyQuote
		a.currentGrid.Count++
	}

	// 4. 更新状态机状态，供下一根 K 线使用
	a.lastActualID = currentActualID

	return completedGrid
}

// loadKlinesFromCSV 从 CSV 文件加载 Kline 数据
// CSV 格式: symbol,period,start_time,dt,open,high,low,close,volume,quote_asset_volume,trades,taker_buy_base_asset_volume,taker_buy_quote_asset_volume,close_time
func loadKlinesFromCSV(path string) ([]Kline, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	// 跳过表头
	_, err = reader.Read()
	if err != nil {
		return nil, fmt.Errorf("读取表头失败: %w", err)
	}

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("读取CSV内容失败: %w", err)
	}

	var klines []Kline
	for i, record := range records {
		if len(record) < 14 {
			return nil, fmt.Errorf("第 %d 行字段不足: 期望 14 个, 实际 %d 个", i+2, len(record))
		}

		openTime, _ := strconv.ParseInt(record[2], 10, 64)
		closeTime, _ := strconv.ParseInt(record[13], 10, 64)
		open, _ := strconv.ParseFloat(record[4], 64)
		high, _ := strconv.ParseFloat(record[5], 64)
		low, _ := strconv.ParseFloat(record[6], 64)
		close, _ := strconv.ParseFloat(record[7], 64)
		volume, _ := strconv.ParseFloat(record[8], 64)
		quoteVolume, _ := strconv.ParseFloat(record[9], 64)
		trades, _ := strconv.ParseInt(record[10], 10, 64)
		takerBuyBase, _ := strconv.ParseFloat(record[11], 64)
		takerBuyQuote, _ := strconv.ParseFloat(record[12], 64)

		k := Kline{
			Symbol:        record[0],
			OpenTime:      openTime,
			CloseTime:     closeTime,
			Open:          open,
			High:          high,
			Low:           low,
			Close:         close,
			Volume:        volume,
			QuoteVolume:   quoteVolume,
			Trades:        trades,
			TakerBuyBase:  takerBuyBase,
			TakerBuyQuote: takerBuyQuote,
		}
		klines = append(klines, k)
	}

	return klines, nil
}

// ==========================
// 实时喂数据模拟测试
// ==========================
func main() {
	// 模拟流式传入的 1 分钟 K 线数据
	// mockStream := []Kline{
	// 	{Symbol: "BTCUSDT", OpenTime: 1700000000, CloseTime: 1700000059, Open: 10000, High: 10050, Low: 9980, Close: 10020, Volume: 5, Trades: 50},
	// 	{Symbol: "BTCUSDT", OpenTime: 1700000060, CloseTime: 1700000119, Open: 10020, High: 10120, Low: 10010, Close: 10110, Volume: 6, Trades: 60}, // 假设这根破位上穿
	// 	{Symbol: "BTCUSDT", OpenTime: 1700000120, CloseTime: 1700000179, Open: 10110, High: 10150, Low: 10090, Close: 10130, Volume: 4, Trades: 40}, // 这根会触发上根切分
	// 	{Symbol: "BTCUSDT", OpenTime: 1700000180, CloseTime: 1700000239, Open: 10130, High: 10160, Low: 10110, Close: 10140, Volume: 7, Trades: 70},
	// }

	// 从 CSV 文件加载 Kline 数据
	klines, err := loadKlinesFromCSV("/tmp/dump.csv")
	if err != nil {
		fmt.Println(err)
		return
	}

	// 为 BTCUSDT 初始化一个状态机
	aggregator := NewGridAggregator("BTCUSDT")

	fmt.Printf("--- 开始实时喂数据 ---%d\n", len(klines))
	for _, kline := range klines {
		// 喂入单根 K 线
		output := aggregator.Feed(kline)

		// 如果有返回值，说明上一个网格 K 线正式结束，可以写入数据库或推送给前端/策略
		if output != nil {
			fmt.Printf("[💥网格结算] GridID: %d, OpenTime: %d, CloseTime: %d, Open: %.2f, Close: %.2f, Count: %d\n",
				output.GridID, output.OpenTime, output.CloseTime, output.Open, output.Close, output.Count)
		}
	}

	fmt.Println("--- 数据流结束（当前未走完的最后一组被保留在内存中，直到下次破位） ---")
}
