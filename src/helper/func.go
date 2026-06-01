package helper

import (
	"fmt"

	"binance.data.sync/src/model"
)

func GetStorage() model.Storage {
	// 读取配置文件
	config := getStorageConfig()
	fmt.Printf("database: %s\n", config.Driver)

	// TODO: 根据配置创建存储实例
	return nil
}

func getStorageConfig() StorageConfig {
	return StorageConfig{}
}

type StorageConfig struct {
	Driver   string `json:"driver"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}
