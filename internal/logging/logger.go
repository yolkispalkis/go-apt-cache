package logging

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Config struct {
	Level      string `koanf:"level" yaml:"level"`
	File       string `koanf:"file" yaml:"file"`
	MaxSizeMB  int    `koanf:"maxSizeMB" yaml:"maxSizeMB"`
	MaxBackups int    `koanf:"maxBackups" yaml:"maxBackups"`
	MaxAgeDays int    `koanf:"maxAgeDays" yaml:"maxAgeDays"`
	NoConsole  bool   `koanf:"noConsole" yaml:"noConsole"`
}

type Logger struct {
	zerolog.Logger
}

func New(cfg Config) (*Logger, error) {
	level := parseLevel(cfg.Level)
	var writers []io.Writer

	if !cfg.NoConsole {
		console := zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.RFC3339,
		}
		writers = append(writers, console)
	}

	if cfg.File != "" {
		if err := os.MkdirAll(filepath.Dir(cfg.File), 0755); err != nil {
			return nil, err
		}
		file := &lumberjack.Logger{
			Filename:   cfg.File,
			MaxSize:    cfg.MaxSizeMB,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAgeDays,
		}
		writers = append(writers, file)
	}

	if len(writers) == 0 {
		writers = append(writers, io.Discard)
	}

	multi := zerolog.MultiLevelWriter(writers...)
	zl := zerolog.New(multi).With().Timestamp().Logger().Level(level)

	return &Logger{zl}, nil
}

// WithComponent создает дочерний логгер с полем "component".
func (l *Logger) WithComponent(name string) *Logger {
	zl := l.With().Str("component", name).Logger()
	return &Logger{zl}
}

// WithContext создает дочерний логгер с дополнительными полями контекста.
// Аргументы должны быть парами: "ключ1", значение1, "ключ2", значение2, ...
func (l *Logger) WithContext(args ...any) *Logger {
	if len(args) == 0 {
		return l
	}
	if len(args)%2 != 0 {
		l.Error().Interface("args", args).Msg("WithContext called with odd number of arguments")
		return l
	}

	ctx := l.With()
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			continue // Пропускаем, если ключ не строка
		}
		ctx.Interface(key, args[i+1])
	}
	zl := ctx.Logger()
	return &Logger{zl}
}

func parseLevel(s string) zerolog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return zerolog.DebugLevel
	case "info":
		return zerolog.InfoLevel
	case "warn", "warning":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	default:
		return zerolog.InfoLevel
	}
}
