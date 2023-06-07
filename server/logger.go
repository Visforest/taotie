package server

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"strings"
)

type FieldsSetter func(ctx context.Context) []zap.Field

type TLoggerConfig struct {
	// Dir is the directory where log files are stored
	Dir string `yaml:"dir"`
	// FileName is the log file name
	FileName string `yaml:"file_name"`
	// Level is the min log level recorded in log file
	Level string `yaml:"level"`
	// MaxSize is the maximum size in megabytes of the log file before it gets rotated.
	MaxSize int `yaml:"max_size"`
	// MaxAge is the maximum number of days to retain old log files based on the
	// timestamp encoded in their filename.  Note that a day is defined as 24
	// hours and may not exactly correspond to calendar days due to daylight
	// savings, leap seconds, etc. The default is not to remove old log files
	// based on age.
	MaxDays int `yaml:"max_days"`
	// MaxBackups is the maximum number of old log files to retain.  The default
	// is to retain all old log files (though MaxAge may still cause them to get
	// deleted.)
	MaxBackups int `yaml:"max_backups"`
	// Compress determines if the rotated log files should be compressed
	// using gzip. The default is not to perform compression.
	Compress bool `yaml:"compress"`
}

type logOption func(e *zap.Logger) *zap.Logger

func withCaller(l *zap.Logger) *zap.Logger {
	return l.WithOptions(zap.AddCaller(), zap.AddCallerSkip(2))
}

func withoutCaller(l *zap.Logger) *zap.Logger {
	return l.WithOptions(zap.WithCaller(false))
}

func withStack(l *zap.Logger) *zap.Logger {
	return l.WithOptions(zap.AddStacktrace(zap.ErrorLevel))
}

var levelMap = map[string]zapcore.Level{
	"debug": zapcore.DebugLevel,
	"info":  zapcore.InfoLevel,
	"warn":  zapcore.WarnLevel,
	"error": zapcore.ErrorLevel,
	"panic": zapcore.PanicLevel,
	"fatal": zapcore.FatalLevel,
}

type TLogger struct {
	*zap.Logger
	fieldsSetter FieldsSetter
}

var ServerLogger *TLogger

func NewLogger(cfg *TLoggerConfig) (*TLogger, error) {
	if !strings.HasSuffix(cfg.Dir, "/") {
		cfg.Dir = cfg.Dir + "/"
	}
	if !strings.HasSuffix(cfg.FileName, ".log") {
		cfg.FileName = cfg.FileName + ".log"
	}
	if err := os.MkdirAll(cfg.Dir, 0666); err != nil {
		return nil, err
	}
	logPath := fmt.Sprintf("%s%s", cfg.Dir, cfg.FileName)

	lumberJackLogger := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    cfg.MaxSize,
		MaxAge:     cfg.MaxDays,
		MaxBackups: cfg.MaxBackups,
		Compress:   cfg.Compress,
	}
	writeSyncer := zapcore.AddSync(lumberJackLogger)

	encoderCfg := zapcore.EncoderConfig{ // set filed name
		MessageKey:    "m",                           // message key
		LevelKey:      "l",                           // log level key
		StacktraceKey: "st",                          // run stacktrace
		CallerKey:     "cl",                          // caller file and line
		TimeKey:       "t",                           // time key
		LineEnding:    zapcore.DefaultLineEnding,     // use \n as line seperator
		EncodeLevel:   zapcore.LowercaseLevelEncoder, // use lower case log level name
		EncodeTime:    zapcore.ISO8601TimeEncoder,    // use time format ISO8601
		EncodeCaller:  zapcore.ShortCallerEncoder,    // use short caller name
	}
	level, ok := levelMap[cfg.Level]
	if !ok {
		level = zapcore.DebugLevel
	}

	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoderCfg), writeSyncer, zap.NewAtomicLevelAt(level))
	logger := zap.New(core)

	return &TLogger{logger, nil}, nil
}

func (l *TLogger) AddFieldsSetter(s FieldsSetter) {
	l.fieldsSetter = s
}

func (l *TLogger) log(ctx context.Context, level zapcore.Level, msg string, err error, opts ...logOption) {
	for _, opt := range opts {
		l.Logger = opt(l.Logger)
	}
	var fields []zap.Field
	if l.fieldsSetter != nil {
		fields = l.fieldsSetter(ctx)
	}
	if err != nil {
		fields = append(fields, zap.NamedError("e", err))
	}
	defer l.Sync()
	l.Log(level, msg, fields...)
}

func (l *TLogger) Debugf(ctx context.Context, msg string, v ...interface{}) {
	l.log(ctx, zap.DebugLevel, fmt.Sprintf(msg, v...), nil, withoutCaller)
}

func (l *TLogger) Infof(ctx context.Context, msg string, v ...interface{}) {
	l.log(ctx, zap.InfoLevel, fmt.Sprintf(msg, v...), nil, withoutCaller)
}

func (l *TLogger) Warnf(ctx context.Context, msg string, v ...interface{}) {
	l.log(ctx, zap.WarnLevel, fmt.Sprintf(msg, v...), nil, withoutCaller)
}

func (l *TLogger) Errorf(ctx context.Context, err error, msg string, v ...interface{}) {
	l.log(ctx, zap.ErrorLevel, fmt.Sprintf(msg, v...), err, withCaller, withStack)
}

func (l *TLogger) Panicf(ctx context.Context, err error, msg string, v ...interface{}) {
	l.log(ctx, zap.PanicLevel, fmt.Sprintf(msg, v...), err, withCaller, withStack)
}

func (l *TLogger) Fatalf(ctx context.Context, err error, msg string, v ...interface{}) {
	l.log(ctx, zap.FatalLevel, fmt.Sprintf(msg, v...), err, withCaller, withStack)
}
