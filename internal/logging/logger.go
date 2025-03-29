// internal/logging/logger.go
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
)

var (
	logger  *slog.Logger
	once    sync.Once
	logSync func() error
)

// Setup initializes the global logger based on config.
func Setup(cfg config.LoggingConfig) error {
	var setupErr error
	once.Do(func() {
		var writers []io.Writer

		if cfg.FilePath != "" {
			lj := &lumberjack.Logger{
				Filename:   cfg.FilePath,
				MaxSize:    cfg.MaxSizeMB,
				MaxBackups: cfg.MaxBackups,
				MaxAge:     cfg.MaxAgeDays,
				Compress:   cfg.Compress,
				LocalTime:  true,
			}
			writers = append(writers, lj)
			logSync = lj.Close
			fmt.Printf("Logging to file: %s (Max Size: %dMB, Max Backups: %d, Max Age: %d days, Compress: %t)\n",
				cfg.FilePath, cfg.MaxSizeMB, cfg.MaxBackups, cfg.MaxAgeDays, cfg.Compress)
		} else {
			logSync = func() error { return nil }
		}

		if !cfg.DisableTerminal {
			writers = append(writers, os.Stdout)
		}

		if len(writers) == 0 {
			writers = append(writers, io.Discard)
			fmt.Println("Warning: No log outputs configured, logging is disabled.")
		}

		multiWriter := io.MultiWriter(writers...)

		level := parseLevel(cfg.Level)

		opts := &slog.HandlerOptions{
			Level:     level,
			AddSource: level <= slog.LevelDebug,
		}

		handler := slog.NewTextHandler(multiWriter, opts)

		logger = slog.New(handler)
		slog.SetDefault(logger)
		logger.Info("Logger initialized", "level", level.String())
	})
	return setupErr
}

func parseLevel(levelStr string) slog.Level {
	switch strings.ToLower(levelStr) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		fmt.Printf("Warning: Unknown log level %q, defaulting to INFO.\n", levelStr)
		return slog.LevelInfo
	}
}

// Sync flushes any buffered log entries (useful mainly for file logging).
func Sync() error {
	if logSync != nil {
		return logSync()
	}
	return nil
}

// --- Convenience functions (using default logger) ---

func Debug(msg string, args ...any) {
	if len(args) > 0 && strings.Contains(msg, "%") {
		slog.Debug(fmt.Sprintf(msg, args...))
	} else {
		slog.Debug(msg, args...)
	}
}

func Info(msg string, args ...any) {
	if len(args) > 0 && strings.Contains(msg, "%") {
		slog.Info(fmt.Sprintf(msg, args...))
	} else {
		slog.Info(msg, args...)
	}
}

func Warn(msg string, args ...any) {
	if len(args) > 0 && strings.Contains(msg, "%") {
		slog.Warn(fmt.Sprintf(msg, args...))
	} else {
		slog.Warn(msg, args...)
	}
}

func Error(msg string, args ...any) {
	if len(args) > 0 && strings.Contains(msg, "%") {
		slog.Error(fmt.Sprintf(msg, args...))
	} else {
		slog.Error(msg, args...)
	}
}

func ErrorE(msg string, err error, args ...any) {
	if len(args) > 0 && strings.Contains(msg, "%") {
		formattedMsg := fmt.Sprintf(msg, args...)
		slog.Error(formattedMsg, "error", err)
	} else {
		allArgs := append([]any{"error", err}, args...)
		slog.Error(msg, allArgs...)
	}
}
