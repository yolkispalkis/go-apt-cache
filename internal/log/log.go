package log

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

type Config struct {
	Level     string `yaml:"level"`
	File      string `yaml:"file"`
	NoConsole bool   `yaml:"noConsole"`
}

type Logger struct{ zerolog.Logger }

func New(cfg Config) (*Logger, error) {
	lvl := parse(cfg.Level)
	var ws []io.Writer
	if !cfg.NoConsole {
		ws = append(ws, zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	}
	if cfg.File != "" {
		if err := os.MkdirAll(filepath.Dir(cfg.File), 0755); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		ws = append(ws, f)
	}
	if len(ws) == 0 {
		ws = append(ws, io.Discard)
	}
	l := zerolog.New(zerolog.MultiLevelWriter(ws...)).With().Timestamp().Logger().Level(lvl)
	return &Logger{l}, nil
}

func (l *Logger) WithComponent(name string) *Logger {
	return &Logger{l.With().Str("component", name).Logger()}
}

func parse(s string) zerolog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return zerolog.DebugLevel
	case "warn", "warning":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	default:
		return zerolog.InfoLevel
	}
}
