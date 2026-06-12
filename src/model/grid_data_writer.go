package model

import (
	"math"
	"strconv"
	"sync"
	"time"
)

// Config 配置参数
const (
	Threshold = 0.01
)

var LogBase = math.Log(1 + Threshold)

type GridVolatilityDataWriter struct {
	gridAggregator map[string]*GridAggregator
	period         string
	volatility     float64
	kind           string
	log_base       float64

	mu sync.Mutex

	// storage persists aggregated kline data to the database
	storage Storage
}

func NewGridVolatilityDataWriter(volatility float64, storage Storage) *GridVolatilityDataWriter {
	// vName := strconv.Itoa(int(volatility * 10))
	// 从AggKline记录查询到最新kline数据
	// 搜索最新一条AggBinanceFutureKline数据的close_time，获得第一条start_time大于close_time的BinanceFutureKline类型数据
	// lastPoint, err := storage.GetLastVolatilityPoint(symbol, vName)
	// if err != nil {
	// 	log.Printf("[volatility_data_writer(%s %s): %s\n", symbol, vName, err.Error())
	// }

	vd := &GridVolatilityDataWriter{
		gridAggregator: make(map[string]*GridAggregator),
		period:         "1m",
		volatility:     volatility,
		kind:           "volatility",
		log_base:       LogBase,
		storage:        storage,
		mu:             sync.Mutex{},
	}
	// if lastPoint != nil && lastPoint.StartTime != 0 {
	// 	// BinanceFutureKline类型
	// 	log.Printf("[volatility_data_writer(%s %s)] loaded last point: start_time: %d\n", symbol, vName, lastPoint.StartTime)
	// 	vd.LoadData(lastPoint)
	// 	return vd
	// }
	return vd
}

// Symbol returns the trading symbol this aggregator tracks.
func (a *GridVolatilityDataWriter) Symbol() string { return "grid" }

// Period returns the aggregation period.
func (a *GridVolatilityDataWriter) Volatility() string { return strconv.Itoa(int(a.volatility * 10)) }

func (a *GridVolatilityDataWriter) LoadData(point *BinanceFutureKline) {

}

// Add inserts a 1m point into the aggregator. When the price change percentage
// exceeds 0.01 %, it produces an aggregated kline, runs all indicators, and
// returns the result. Returns nil if the threshold has not been reached.
func (a *GridVolatilityDataWriter) Add(point *FutureKlinePoint) (*AggregatedFutureKline, error) {
	symbol := point.Symbol
	a.mu.Lock()
	defer a.mu.Unlock()
	gridAggregator, ok := a.gridAggregator[symbol]
	if !ok {
		gridAggregator = NewGridAggregator(symbol)
		a.gridAggregator[symbol] = gridAggregator
	}
	agg := gridAggregator.Feed(point)
	if agg != nil {
		err := a.saveAggregated(agg)
		if err != nil {
			return nil, err
		}
		retVal := &AggregatedFutureKline{
			Symbol:                   agg.Symbol,
			StartTime:                agg.StartTime,
			CloseTime:                agg.CloseTime,
			Open:                     agg.Open,
			High:                     agg.High,
			Low:                      agg.Low,
			Close:                    agg.Close,
			Volume:                   agg.Volume,
			QuoteAssetVolume:         agg.QuoteAssetVolume,
			Trades:                   agg.Trades,
			TakerBuyBaseAssetVolume:  agg.TakerBuyBaseAssetVolume,
			TakerBuyQuoteAssetVolume: agg.TakerBuyQuoteAssetVolume,
			Count:                    1,
			GridID:                   agg.GridID,
		}
		return retVal, nil
	}

	return nil, nil

}

// saveAggregated writes the aggregated kline to the AggBinanceSpotKline table.
func (a *GridVolatilityDataWriter) saveAggregated(agg *AggBinanceFutureKline) error {
	agg.DateTime = DateTimeMillis(time.Now().UnixMilli())

	if err := a.storage.CommitAggKline(agg); err != nil {
		return err
	}
	return nil
}

// GridAggregator 状态机：每个交易对（Symbol）需要独立维护一个实例
type GridAggregator struct {
	symbol       string
	currentGrid  *AggBinanceFutureKline // 当前正在累加的网格K线缓存
	lastActualID int64                  // 对应 Python 中 grid_id_actual 的最新状态
	isFirstKline bool                   // 是否是第一根K线
}

// NewGridAggregator 创建一个状态机实例
func NewGridAggregator(symbol string) *GridAggregator {
	return &GridAggregator{
		symbol:       symbol,
		isFirstKline: true,
	}
}

// Feed 核心逻辑：每来一根K线，调用一次该函数。
// 如果该K线导致了网格破位切换，则会返回一个聚合完毕的 *GridKline，否则返回 nil。
func (a *GridAggregator) Feed(point *FutureKlinePoint) *AggBinanceFutureKline {
	openGrid := calcGrid(point.Open)
	closeGrid := calcGrid(point.Close)

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
		a.currentGrid = &AggBinanceFutureKline{
			Symbol:                   point.Symbol,
			StartTime:                point.StartTime,
			CloseTime:                point.CloseTime,
			Open:                     point.Open,
			High:                     point.High,
			Low:                      point.Low,
			Close:                    point.Close,
			Volume:                   point.Volume,
			QuoteAssetVolume:         point.QuoteAssetVolume,
			Trades:                   point.Trades,
			TakerBuyBaseAssetVolume:  point.TakerBuyBaseAssetVolume,
			TakerBuyQuoteAssetVolume: point.TakerBuyQuoteAssetVolume,
			Count:                    1,
			GridID:                   actualID,
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
	var completedGrid *AggBinanceFutureKline

	if a.lastActualID != a.currentGrid.GridID {
		// 触发切换：把当前的 Buffer 作为已完成的网格 K 线吐出
		completedGrid = a.currentGrid

		// 用当前这根 K 线创建全新的网格组，其 GridID 继承上一次的 actualID
		a.currentGrid = &AggBinanceFutureKline{
			Symbol:                   point.Symbol,
			StartTime:                point.StartTime,
			CloseTime:                point.CloseTime,
			Open:                     point.Open,
			High:                     point.High,
			Low:                      point.Low,
			Close:                    point.Close,
			Volume:                   point.Volume,
			QuoteAssetVolume:         point.QuoteAssetVolume,
			Trades:                   point.Trades,
			TakerBuyBaseAssetVolume:  point.TakerBuyBaseAssetVolume,
			TakerBuyQuoteAssetVolume: point.TakerBuyQuoteAssetVolume,
			Count:                    1,
			GridID:                   a.lastActualID, // 承接 shift(1) 后的 ID
		}

	} else {
		// 未触发切换：更新当前的 Buffer (累加 High, Low, Volume, 计数等)
		a.currentGrid.CloseTime = point.CloseTime
		a.currentGrid.Close = point.Close
		if point.High > a.currentGrid.High {
			a.currentGrid.High = point.High
		}
		if point.Low < a.currentGrid.Low {
			a.currentGrid.Low = point.Low
		}
		a.currentGrid.Volume += point.Volume
		a.currentGrid.QuoteAssetVolume += point.QuoteAssetVolume
		a.currentGrid.Trades += point.Trades
		a.currentGrid.TakerBuyBaseAssetVolume += point.TakerBuyBaseAssetVolume
		a.currentGrid.TakerBuyQuoteAssetVolume += point.TakerBuyQuoteAssetVolume
		a.currentGrid.Count++
	}

	// 4. 更新状态机状态，供下一根 K 线使用
	a.lastActualID = currentActualID

	return completedGrid
}

// calcGrid 计算价格对应的网格线（浮点）
func calcGrid(price float64) float64 {
	return math.Log(price) / LogBase
}
