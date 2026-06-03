package main

import (
	"fmt"

	"binance.data.sync/src/helper"
	"binance.data.sync/src/model"
)

func main() {
	fmt.Println("initializing database tables...")

	// GetStorage creates the DB connection and auto-migrates BinanceSpotKline
	storage := helper.GetStorage()
	db := storage.GetDB()

	// Auto migrate the schema
	if err := db.AutoMigrate(&model.BinanceSpotKline{}); err != nil {
		panic(fmt.Errorf("failed to auto migrate: %w", err))
	}

	if err := db.AutoMigrate(&model.AggBinanceSpotKline{}); err != nil {
		panic(fmt.Errorf("failed to auto migrate: %w", err))
	}

	fmt.Println("database table 'binance_spot_kline' created successfully")
}
