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
	logSync func() error // Function to sync file logger if used
)

// Setup initializes the global logger based on config.
func Setup(cfg config.LoggingConfig) error {
	var setupErr error
	once.Do(func() {
		var writers []io.Writer

		// File Writer (using lumberjack for rotation)
		if cfg.FilePath != "" {
			lj := &lumberjack.Logger{
				Filename:   cfg.FilePath,
				MaxSize:    cfg.MaxSizeMB, // megabytes
				MaxBackups: cfg.MaxBackups,
				MaxAge:     cfg.MaxAgeDays, //days
				Compress:   cfg.Compress,   // disabled by default
				LocalTime:  true,           // Use local time for timestamps in filenames
			}
			writers = append(writers, lj)
			logSync = lj.Close // Assign lumberjack's Close for syncing/closing
			fmt.Printf("Logging to file: %s (Max Size: %dMB, Max Backups: %d, Max Age: %d days, Compress: %t)\n",
				cfg.FilePath, cfg.MaxSizeMB, cfg.MaxBackups, cfg.MaxAgeDays, cfg.Compress)
		} else {
			logSync = func() error { return nil } // No-op sync if no file
		}

		// Terminal Writer
		if !cfg.DisableTerminal {
			writers = append(writers, os.Stdout)
		}

		if len(writers) == 0 {
			// Should not happen if config validation passes, but safety first
			writers = append(writers, io.Discard)
			fmt.Println("Warning: No log outputs configured, logging is disabled.")
		}

		multiWriter := io.MultiWriter(writers...)

		level := parseLevel(cfg.Level)

		opts := &slog.HandlerOptions{
			Level:     level,
			AddSource: level <= slog.LevelDebug, // Only add source if debug level
		}

		// Use TextHandler for more human-readable logs, JSONHandler for machine parsing
		handler := slog.NewTextHandler(multiWriter, opts)
		// handler := slog.NewJSONHandler(multiWriter, opts)

		logger = slog.New(handler)
		slog.SetDefault(logger) // Set as default for convenience
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
	slog.Debug(msg, args...)
}

func Info(msg string, args ...any) {
	slog.Info(msg, args...)
}

func Warn(msg string, args ...any) {
	slog.Warn(msg, args...)
}

func Error(msg string, args ...any) {
	slog.Error(msg, args...)
}

// ErrorE is a helper to log an error object along with a message.
func ErrorE(msg string, err error, args ...any) {
	allArgs := append([]any{"error", err}, args...)
	slog.Error(msg, allArgs...)
}
