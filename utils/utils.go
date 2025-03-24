package utils

import (
	"strconv"
	"time"
)

func GenTimeStr() string {
	return strconv.Itoa(int(time.Now().UnixNano()))
}
