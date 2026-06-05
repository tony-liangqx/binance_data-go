package helper

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"binance.data.sync/src/model"

	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

// StorageConfig database connection configuration
type StorageConfig struct {
	Driver   string `json:"driver"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}

// Subscription represents a single symbol and period pair
type Subscription struct {
	Symbol string `json:"symbol"`
	Period string `json:"period"`
}

// AppConfig application configuration
type AppConfig struct {
	Storage       StorageConfig  `json:"storage"`
	Subscriptions []Subscription `json:"subscriptions"`
}

// GetStorage creates a database connection using GORM (ClickHouse) and returns a Storage instance.
// Reads configuration from config.json.
func GetStorage() model.Storage {
	config := getAppConfig()
	fmt.Printf("database driver: %s, host: %s:%d\n", config.Storage.Driver, config.Storage.Host, config.Storage.Port)

	var dialector gorm.Dialector
	switch config.Storage.Driver {
	case "clickhouse":
		dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s?dial_timeout=10s&read_timeout=20s",
			config.Storage.Username,
			config.Storage.Password,
			config.Storage.Host,
			config.Storage.Port,
			config.Storage.Database,
		)
		dialector = clickhouse.Open(dsn)
	default:
		panic(fmt.Sprintf("unsupported database driver: %s", config.Storage.Driver))
	}

	db, err := gorm.Open(dialector, &gorm.Config{
		SkipDefaultTransaction: true,
	})
	if err != nil {
		panic(fmt.Errorf("failed to connect database: %w", err))
	}

	// Configure connection pool
	sqlDB, err := db.DB()
	if err != nil {
		panic(fmt.Errorf("failed to get underlying DB: %w", err))
	}
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)

	return model.NewGormStorage(db)
}

// GetSubscriptions reads the list of symbol/period pairs from config
func GetSubscriptions() []Subscription {
	return getAppConfig().Subscriptions
}

// getAppConfig reads and parses config.json
func getAppConfig() AppConfig {
	data, err := os.ReadFile("config.json")
	if err != nil {
		panic(fmt.Errorf("failed to read config.json: %w", err))
	}

	var config AppConfig
	if err := json.Unmarshal(data, &config); err != nil {
		panic(fmt.Errorf("failed to parse config.json: %w", err))
	}

	return config
}
