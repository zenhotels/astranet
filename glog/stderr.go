package glog

import "log"

type stdLogger struct {
	Verbosity int
	Log       *log.Logger
}

func (self *stdLogger) VLog(v int, c func(*log.Logger)) {
	if v > self.Verbosity {
		return
	}
	c(self.Log)
}

func (self *stdLogger) Panic(args ...interface{}) {
	self.Log.Panicln(args...)
}

func New(level int, l *log.Logger) Logger {
	return &stdLogger{
		Verbosity: level,
		Log:       l,
	}
}
