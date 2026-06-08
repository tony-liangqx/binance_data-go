CREATE TABLE `binance_futures_kline` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

  `symbol` VARCHAR(20) NOT NULL COMMENT '交易对',
  `period` VARCHAR(10) NOT NULL COMMENT '周期 1m/5m/1h/1d',
  `start_time` BIGINT NOT NULL COMMENT 'K线起始时间(毫秒)',
  `dt` DATETIME(3) NOT NULL COMMENT '时间',

  `open` DECIMAL(22,8) NOT NULL,
  `high` DECIMAL(22,8) NOT NULL,
  `low` DECIMAL(22,8) NOT NULL,
  `close` DECIMAL(22,8) NOT NULL,
  `volume` DECIMAL(32,8) NOT NULL,

  `close_time` BIGINT NOT NULL,
  `quote_asset_volume` DECIMAL(32,8) NOT NULL,
  `trades` INT UNSIGNED NOT NULL,

  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_symbol_period_time` (`symbol`,`period`,`start_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '币安K线(兼容Websocket+REST)';
