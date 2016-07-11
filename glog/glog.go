package glog

import "log"

type Logger interface {
	VLog(int, func(*log.Logger))
	Panic(v ...interface{})
}
