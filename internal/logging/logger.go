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

// Config хранит настройки логгирования.
type Config struct {
	Level      string `koanf:"level"`
	File       string `koanf:"file"`
	MaxSizeMB  int    `koanf:"maxSizeMB"`
	MaxBackups int    `koanf:"maxBackups"`
	MaxAgeDays int    `koanf:"maxAgeDays"`
	NoConsole  bool   `koanf:"noConsole"`
}

// Logger - это обертка над zerolog.Logger для добавления контекста.
type Logger struct {
	zerolog.Logger
}

// New создает и настраивает новый логгер.
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

// WithComponent добавляет имя компонента в контекст логгера.
func (l *Logger) WithComponent(name string) *Logger {
	zl := l.With().Str("component", name).Logger()
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
