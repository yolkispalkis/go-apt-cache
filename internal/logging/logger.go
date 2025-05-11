package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	LevelDebug = "debug"
	LevelInfo  = "info"
	LevelWarn  = "warn"
	LevelError = "error"
	LevelFatal = "fatal"
	LevelPanic = "panic"
)

type Config struct {
	Level           string `json:"level"`
	Path            string `json:"filePath"`
	DisableTerminal bool   `json:"disableTerminal"`
	MaxSizeMB       int    `json:"maxSizeMB"`
	MaxBackups      int    `json:"maxBackups"`
	MaxAgeDays      int    `json:"maxAgeDays"`
	Compress        bool   `json:"compress"`
}

var (
	logCloser             io.Closer
	consoleOffUserSetting bool
	loggerMux             sync.RWMutex
)

func init() {
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
	basicLogger := zerolog.New(consoleWriter).With().Timestamp().Logger().Level(zerolog.InfoLevel)
	log.Logger = basicLogger
}

func Setup(cfg Config) error {
	var setupErr error

	loggerMux.Lock()
	defer loggerMux.Unlock()

	writers := make([]io.Writer, 0, 2)
	consoleOffUserSetting = cfg.DisableTerminal

	if logCloser != nil {
		currentLogCloser := logCloser
		logCloser = nil
		if err := currentLogCloser.Close(); err != nil {
			log.Logger.Warn().Err(err).Msg("Error closing previous log file writer")
		}
	}

	if cfg.Path != "" {
		absPath, pathErr := filepath.Abs(cfg.Path)
		if pathErr != nil {
			log.Logger.Error().Err(pathErr).Str("path", cfg.Path).Msg("Failed to get absolute path for log file, file logging disabled for this setup.")
			setupErr = pathErr
		} else {
			dirErr := os.MkdirAll(filepath.Dir(absPath), 0750)
			if dirErr != nil {
				log.Logger.Error().Err(dirErr).Str("dir", filepath.Dir(absPath)).Msg("Failed to create log directory, file logging disabled for this setup.")
				setupErr = dirErr
			} else {
				lj := &lumberjack.Logger{
					Filename:   absPath,
					MaxSize:    cfg.MaxSizeMB,
					MaxBackups: cfg.MaxBackups,
					MaxAge:     cfg.MaxAgeDays,
					Compress:   cfg.Compress,
					LocalTime:  true,
				}
				writers = append(writers, lj)
				logCloser = lj
			}
		}
	}

	if !consoleOffUserSetting {
		cw := zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.RFC3339,
			FormatLevel: func(i any) string {
				if l, ok := i.(string); ok {
					return strings.ToUpper(fmt.Sprintf("[%s]", l))
				}
				return fmt.Sprintf("[%v]", i)
			},
			FormatCaller: func(i any) string {
				if s, ok := i.(string); ok && s != "" {
					return fmt.Sprintf("<%s>", s)
				}
				return ""
			},
		}
		if !isTerminal() {
			cw.NoColor = true
		}
		writers = append(writers, cw)
	}

	if len(writers) == 0 {
		writers = append(writers, io.Discard)
	}

	level, parseErr := ParseLevel(cfg.Level)
	if parseErr != nil {
		log.Logger.Warn().Err(parseErr).Str("configured_level", cfg.Level).Msg("Invalid log level in config, defaulting to INFO.")
		level = zerolog.InfoLevel

		if setupErr == nil {
			setupErr = parseErr
		}
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.CallerMarshalFunc = func(_ uintptr, file string, line int) string {
		short := file
		markers := []string{"/go-apt-cache/", "/internal/", "/pkg/"}
		for _, marker := range markers {
			if idx := strings.LastIndex(file, marker); idx != -1 {
				short = file[idx+1:]
				break
			}
		}
		if short == file {
			short = filepath.Base(file)
		}
		return fmt.Sprintf("%s:%d", short, line)
	}

	multiWriter := zerolog.MultiLevelWriter(writers...)
	builder := zerolog.New(multiWriter).With().Timestamp()

	if level <= zerolog.DebugLevel {
		builder = builder.Caller()
	}

	newLogger := builder.Logger().Level(level)
	log.Logger = newLogger

	if cfg.Path != "" && logCloser != nil {
		absPath, _ := filepath.Abs(cfg.Path)
		log.Logger.Info().Str("path", absPath).
			Int("max_size_mb", cfg.MaxSizeMB).
			Int("max_backups", cfg.MaxBackups).
			Int("max_age_days", cfg.MaxAgeDays).
			Bool("compress", cfg.Compress).
			Msg("File logging configured.")
	}
	if !consoleOffUserSetting {
		log.Logger.Info().Msg("Console logging enabled.")
	}

	log.Logger.Info().Msgf("Logger setup complete. Effective global level: %s", log.Logger.GetLevel().String())

	return setupErr
}

func ParseLevel(s string) (zerolog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case LevelDebug:
		return zerolog.DebugLevel, nil
	case LevelInfo:
		return zerolog.InfoLevel, nil
	case LevelWarn, "warning":
		return zerolog.WarnLevel, nil
	case LevelError:
		return zerolog.ErrorLevel, nil
	case LevelFatal:
		return zerolog.FatalLevel, nil
	case LevelPanic:
		return zerolog.PanicLevel, nil
	default:
		return zerolog.NoLevel, fmt.Errorf("unknown log level: %q (valid: debug, info, warn, error, fatal, panic)", s)
	}
}

func Sync() error {
	loggerMux.RLock()
	defer loggerMux.RUnlock()
	if logCloser != nil {
		return logCloser.Close()
	}
	return nil
}

func L() zerolog.Logger {
	loggerMux.RLock()
	defer loggerMux.RUnlock()
	return log.Logger
}

func isTerminal() bool {
	info, _ := os.Stderr.Stat()
	if info == nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}
