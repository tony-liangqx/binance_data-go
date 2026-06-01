package model

import "gorm.io/gorm"

type Storage interface {
	Commit(point *SpotKlinePoint) error
	GetLastTimeStamp(symbol string, period string) (int64, error)
	GetDB() *gorm.DB
}
