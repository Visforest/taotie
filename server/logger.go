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

//var ServerLogger *zerolog.Logger
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
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

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
	case "panic":
		lvl = zerolog.PanicLevel
	case "fatal":
		lvl = zerolog.FatalLevel
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

func (l *TLogger) Debugf(ctx context.Context, msg string, v ...interface{}) {
	var reqId string
	r := ctx.Value("requestId")
	if r != nil {
		reqId = r.(string)
	}
	if reqId != "" {
		// 同时记录请求 ID
		l.Debug().Timestamp().Str("rid", reqId).Msgf(msg, v)
	} else {
		l.Debug().Timestamp().Msgf(msg, v)
	}
}

func (l *TLogger) Infof(ctx context.Context, msg string, v ...interface{}) {
	var reqId string
	r := ctx.Value("requestId")
	if r != nil {
		reqId = r.(string)
	}
	if reqId != "" {
		// 同时记录请求 ID
		l.Info().Timestamp().Str("rid", reqId).Msgf(msg, v)
	} else {
		l.Info().Timestamp().Msgf(msg, v)
	}
}

func (l *TLogger) Warnf(ctx context.Context, msg string, v ...interface{}) {
	var reqId string
	r := ctx.Value("requestId")
	if r != nil {
		reqId = r.(string)
	}
	if reqId != "" {
		// 同时记录请求 ID
		l.Warn().Timestamp().Str("rid", reqId).Msgf(msg, v)
	} else {
		l.Warn().Timestamp().Msgf(msg, v)
	}
}

func (l *TLogger) Fatalf(ctx context.Context, err error, msg string, v ...interface{}) {
	var reqId string
	r := ctx.Value("requestId")
	if r != nil {
		reqId = r.(string)
	}
	if reqId != "" {
		// 同时记录请求 ID
		l.Fatal().Timestamp().Str("rid", reqId).Err(err).Msgf(msg, v)
	} else {
		l.Fatal().Timestamp().Err(err).Msgf(msg, v)
	}
}

func (l *TLogger) Panicf(ctx context.Context, err error, msg string, v ...interface{}) {
	var reqId string
	r := ctx.Value("requestId")
	if r != nil {
		reqId = r.(string)
	}
	if reqId != "" {
		// 同时记录请求 ID
		l.Panic().Timestamp().Str("rid", reqId).Err(err).Msgf(msg, v)
	} else {
		l.Panic().Timestamp().Err(err).Msgf(msg, v)
	}
}
