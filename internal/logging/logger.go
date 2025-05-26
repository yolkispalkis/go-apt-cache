package logging

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Config struct {
	Level      string `json:"level"`
	File       string `json:"file,omitempty"`
	MaxSizeMB  int    `json:"maxSizeMB,omitempty"`
	MaxBackups int    `json:"maxBackups,omitempty"`
	MaxAgeDays int    `json:"maxAgeDays,omitempty"`
	NoConsole  bool   `json:"noConsole,omitempty"`
}

func Setup(cfg Config) error {
	level := parseLevel(cfg.Level)

	var writers []io.Writer

	// Console writer
	if !cfg.NoConsole {
		console := zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: "15:04:05",
		}
		writers = append(writers, console)
	}

	// File writer
	if cfg.File != "" {
		if err := os.MkdirAll(filepath.Dir(cfg.File), 0755); err != nil {
			return err
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
	log.Logger = zerolog.New(multi).With().Timestamp().Logger().Level(level)

	return nil
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

func L() zerolog.Logger {
	return log.Logger
}
