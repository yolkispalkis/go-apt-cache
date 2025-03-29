// internal/logging/logger.go
package logging

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	logSync func() error
)

// Уровни логирования
const (
	LevelDebug = "debug"
	LevelInfo  = "info"
	LevelWarn  = "warn"
	LevelError = "error"
)

// LoggingConfig содержит настройки логирования
type LoggingConfig struct {
	Level           string
	FilePath        string
	DisableTerminal bool
	MaxSizeMB       int
	MaxBackups      int
	MaxAgeDays      int
	Compress        bool
}

// Setup инициализирует глобальный логгер на основе конфигурации
func Setup(cfg LoggingConfig) error {
	// Настройка вывода логов
	var writers []io.Writer

	// Файловый лог
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

	// Консольный вывод
	if !cfg.DisableTerminal {
		consoleWriter := zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
			NoColor:    false,
		}
		writers = append(writers, consoleWriter)
	}

	// Проверка наличия выводов
	if len(writers) == 0 {
		writers = append(writers, io.Discard)
		fmt.Println("Warning: No log outputs configured, logging is disabled.")
	}

	// Настройка уровня логирования
	level := parseLevel(cfg.Level)
	zerolog.SetGlobalLevel(level)

	// Настройка вывода
	multi := zerolog.MultiLevelWriter(writers...)

	// Форматирование для консоли
	zerolog.TimeFieldFormat = time.RFC3339

	// Установка глобального логгера
	log.Logger = zerolog.New(multi).With().Timestamp().Caller().Logger()

	log.Info().Msgf("Logger initialized with level: %s", cfg.Level)

	return nil
}

func parseLevel(levelStr string) zerolog.Level {
	switch strings.ToLower(levelStr) {
	case "debug":
		return zerolog.DebugLevel
	case "info":
		return zerolog.InfoLevel
	case "warn", "warning":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	default:
		fmt.Printf("Warning: Unknown log level %q, defaulting to INFO.\n", levelStr)
		return zerolog.InfoLevel
	}
}

// Sync сбрасывает буферизованные записи лога
func Sync() error {
	if logSync != nil {
		return logSync()
	}
	return nil
}

// --- Вспомогательные функции ---

func Debug(msg string, args ...any) {
	if len(args) > 0 && strings.Contains(msg, "%") {
		log.Debug().Msgf(msg, args...)
	} else if len(args) > 0 {
		log.Debug().Fields(argsToMap(args)).Msg(msg)
	} else {
		log.Debug().Msg(msg)
	}
}

func Info(msg string, args ...any) {
	if len(args) > 0 && strings.Contains(msg, "%") {
		log.Info().Msgf(msg, args...)
	} else if len(args) > 0 {
		log.Info().Fields(argsToMap(args)).Msg(msg)
	} else {
		log.Info().Msg(msg)
	}
}

func Warn(msg string, args ...any) {
	if len(args) > 0 && strings.Contains(msg, "%") {
		log.Warn().Msgf(msg, args...)
	} else if len(args) > 0 {
		log.Warn().Fields(argsToMap(args)).Msg(msg)
	} else {
		log.Warn().Msg(msg)
	}
}

func Error(msg string, args ...any) {
	if len(args) > 0 && strings.Contains(msg, "%") {
		log.Error().Msgf(msg, args...)
	} else if len(args) > 0 {
		log.Error().Fields(argsToMap(args)).Msg(msg)
	} else {
		log.Error().Msg(msg)
	}
}

func ErrorE(msg string, err error, args ...any) {
	if len(args) > 0 && strings.Contains(msg, "%") {
		log.Error().Err(err).Msgf(msg, args...)
	} else if len(args) > 0 {
		log.Error().Err(err).Fields(argsToMap(args)).Msg(msg)
	} else {
		log.Error().Err(err).Msg(msg)
	}
}

// argsToMap преобразует аргументы вида [key1, val1, key2, val2] в map[string]interface{}
func argsToMap(args []any) map[string]interface{} {
	result := make(map[string]interface{})

	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			if key, ok := args[i].(string); ok {
				result[key] = args[i+1]
			}
		}
	}

	return result
}
