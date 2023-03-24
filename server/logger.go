package server

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
	"os"
	"strings"
	"time"
)

type TLogger struct {
	zerolog.Logger
}

type LogOption func(e *zerolog.Event) *zerolog.Event

func WithCaller(e *zerolog.Event) *zerolog.Event {
	return e.Caller()
}

var ServerLogger *TLogger
var RetryLogger *TLogger

func InitLogger(fileName string) (*TLogger, error) {
	if !strings.HasSuffix(GlbConfig.Log.LogDir, "/") {
		GlbConfig.Log.LogDir = GlbConfig.Log.LogDir + "/"
	}
	if !strings.HasSuffix(fileName, ".log") {
		fileName = fileName + ".log"
	}
	if err := os.MkdirAll(GlbConfig.Log.LogDir, 0666); err != nil {
		return nil, err
	}
	logPath := fmt.Sprintf("%s%s", GlbConfig.Log.LogDir, fileName)
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	if err != nil {
		return nil, err
	}

	zerolog.TimestampFieldName = "t"
	zerolog.LevelFieldName = "l"
	zerolog.MessageFieldName = "m"
	zerolog.ErrorFieldName = "e"
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs // 使用毫秒记录时间戳

	var lvl zerolog.Level
	switch GlbConfig.Log.LogLevel {
	case "debug":
		lvl = zerolog.DebugLevel
	case "info":
		lvl = zerolog.InfoLevel
	case "warn":
		lvl = zerolog.WarnLevel
	case "err":
		lvl = zerolog.ErrorLevel
	case "fatal":
		lvl = zerolog.FatalLevel
	case "panic":
		lvl = zerolog.PanicLevel
	case "disable":
		lvl = zerolog.Disabled
	default:
		lvl = zerolog.Disabled
	}
	zerolog.SetGlobalLevel(lvl)

	wr := diode.NewWriter(f, 1000, 10*time.Millisecond, func(missed int) {
		fmt.Printf("Logger Dropped %d messages", missed)
	})
	logger := TLogger{zerolog.New(wr)}
	return &logger, nil
}

func (l *TLogger) log(ctx context.Context, level zerolog.Level, msg string, err error, opts ...LogOption) {
	e := l.WithLevel(level)
	for _, opt := range opts {
		e = opt(e)
	}
	if err != nil {
		e = e.Err(err)
	}
	var reqId string
	r := ctx.Value("requestId")
	if r != nil {
		reqId = r.(string)
	}
	if reqId != "" {
		e = e.Str("rid", reqId)
	}
	e.Timestamp().Msg(msg)
}

func (l *TLogger) Debugf(ctx context.Context, msg string, v ...interface{}) {
	l.log(ctx, zerolog.DebugLevel, fmt.Sprintf(msg, v), nil)
}

func (l *TLogger) Infof(ctx context.Context, msg string, v ...interface{}) {
	l.log(ctx, zerolog.InfoLevel, fmt.Sprintf(msg, v), nil)
}

func (l *TLogger) Warnf(ctx context.Context, msg string, v ...interface{}) {
	l.log(ctx, zerolog.WarnLevel, fmt.Sprintf(msg, v), nil, WithCaller)
}

func (l *TLogger) Errorf(ctx context.Context, err error, msg string, v ...interface{}) {
	l.log(ctx, zerolog.ErrorLevel, fmt.Sprintf(msg, v), err, WithCaller)
}

func (l *TLogger) Fatalf(ctx context.Context, err error, msg string, v ...interface{}) {
	l.log(ctx, zerolog.FatalLevel, fmt.Sprintf(msg, v), err, WithCaller)
}

func (l *TLogger) Panicf(ctx context.Context, err error, msg string, v ...interface{}) {
	l.log(ctx, zerolog.PanicLevel, fmt.Sprintf(msg, v), err, WithCaller)
}
