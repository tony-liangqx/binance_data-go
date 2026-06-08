WITH
RawData AS (
    SELECT
        symbol,
        period,
        start_time,
        close_time,
        close,
        volume,
        -- 关键：按 币种+周期 分区，单独取上一条，不跨币计算
        lag(close, 1) OVER (PARTITION BY symbol, period ORDER BY start_time) AS prev_close
    FROM binance_futures_kline
),
MarkBuckets AS (
    SELECT
        symbol,
        period,
        start_time,
        close_time,
        close,
        volume,
        prev_close,
        -- 没有prev_close → 冷启动 → 标记为0（不触发新K线）
        -- 有prev_close且波动≥1% → 标记为1（触发新K线）
        if(abs(close - prev_close) / prev_close >= 0.01, 1, 0) AS IsNewBucket
    FROM RawData  WHERE prev_close > 0
),
GroupedData AS (
    SELECT
        symbol,
        period,
        start_time,
        close_time,
        close,
        volume,
        prev_close,
        -- 关键：按 币种+周期 分区生成分组ID
        sum(IsNewBucket) OVER (PARTITION BY symbol, period ORDER BY start_time ASC ROWS UNBOUNDED PRECEDING) AS GroupID
    FROM MarkBuckets
)
SELECT
    GroupID,
    symbol,
    period,
    min(start_time) AS StartTime,
    max(close_time) AS EndTime,
    argMin(close, start_time) AS Open,
    max(close) AS High,
    min(close) AS Low,
    argMax(close, start_time) AS Close,
    sum(volume) AS totalVolume
FROM GroupedData
-- 关键修复：必须 GROUP BY 这三个字段
GROUP BY GroupID, symbol, period
ORDER BY symbol, period, StartTime ASC;

--      ┌─GroupID─┬─symbol──┬─period─┬─────StartTime─┬───────EndTime─┬─────Open─┬─────High─┬──────Low─┬────Close─┬────────totalVolume─┐
--  97. │      47 │ ETHUSDT │ 1m     │ 1780650540000 │ 1780650719999 │  1672.96 │  1673.34 │  1672.96 │  1673.08 │ 1618.9643999999998 │
--  95. │      45 │ ETHUSDT │ 1m     │ 1780650120000 │ 1780650359999 │  1669.99 │  1669.99 │  1668.37 │  1669.17 │          2081.9371 │
--   8. │       8 │ BTCUSDT │ 1m     │ 1780645620000 │ 1780645739999 │ 62916.32 │  62955.9 │ 62916.32 │  62955.9 │ 110.02562999999999 │
--   9. │       9 │ BTCUSDT │ 1m     │ 1780645740000 │ 1780645859999 │ 62850.01 │ 62850.01 │ 62822.01 │ 62822.01 │ 105.22704999999999 │
--
--     ┌─GroupID─┬─symbol──┬─period─┬─────StartTime─┬───────EndTime─┬─────Open─┬─────High─┬──────Low─┬────Close─┬────────totalVolume─┐
--  1. │       0 │ BTCUSDT │ 1m     │ 1780644660000 │ 1780655759999 │ 62194.62 │ 63130.38 │ 62194.62 │ 62538.88 │ 4417.9909800000005 │
--  2. │       0 │ ETHUSDT │ 1m     │ 1780644660000 │ 1780655759999 │   1654.6 │   1687.1 │   1654.6 │  1678.66 │ 127588.93320000003 │
--     └─────────┴─────────┴────────┴───────────────┴───────────────┴──────────┴──────────┴──────────┴──────────┴────────────────────┘

WITH
RawData AS (
    SELECT
        symbol,
        period,
        start_time,
        close_time,
        close,
        volume,
        -- 关键：按 币种+周期 分区，单独取上一条，不跨币计算
        lag(close, 1) OVER (PARTITION BY symbol, period ORDER BY start_time) AS prev_close
    FROM binance_futures_kline
)
SELECT
    symbol,
    period,
    start_time,
    close_time,
    close,
    volume,
    prev_close,
    -- 没有prev_close → 冷启动 → 标记为0（不触发新K线）
    -- 有prev_close且波动≥1% → 标记为1（触发新K线）
    if(abs(close - prev_close) / prev_close >= 0.01, 1, 0) AS IsNewBucket
FROM RawData  WHERE prev_close > 0



WITH
RawData AS (
    SELECT
        symbol,
        period,
        start_time,
        close_time,
        close,
        volume,
        -- 关键：按 币种+周期 分区，单独取上一条，不跨币计算
        lag(close, 1) OVER (PARTITION BY symbol, period ORDER BY start_time) AS prev_close
    FROM binance_futures_kline
),
MarkBuckets AS (
    SELECT
        symbol,
        period,
        start_time,
        close_time,
        close,
        volume,
        prev_close,
        -- 没有prev_close → 冷启动 → 标记为0（不触发新K线）
        -- 有prev_close且波动≥1% → 标记为1（触发新K线）
        if(abs(close - prev_close) / prev_close >= 0.001, 1, 0) AS IsNewBucket
    FROM RawData  WHERE prev_close > 0
)
SELECT
    symbol,
    period,
    start_time,
    close_time,
    close,
    volume,
    prev_close,
    -- 关键：按 币种+周期 分区生成分组ID
    sum(IsNewBucket) OVER (PARTITION BY symbol, period ORDER BY start_time ASC ROWS UNBOUNDED PRECEDING) AS GroupID
FROM MarkBuckets;
