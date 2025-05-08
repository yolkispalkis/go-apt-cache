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
	globalL    zerolog.Logger
	logCloser  io.Closer
	setupOnce  sync.Once
	consoleOff bool
)

func Setup(cfg Config) error {
	var err error
	setupOnce.Do(func() {
		writers := make([]io.Writer, 0, 2)
		consoleOff = cfg.DisableTerminal

		if cfg.Path != "" {
			absPath, pathErr := filepath.Abs(cfg.Path)
			if pathErr != nil {
				err = fmt.Errorf("abs log path for '%s': %w", cfg.Path, pathErr)
				return
			}
			if dirErr := os.MkdirAll(filepath.Dir(absPath), 0750); dirErr != nil {
				err = fmt.Errorf("create log dir '%s': %w", filepath.Dir(absPath), dirErr)
				return
			}
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

			fmt.Fprintf(os.Stderr, "Logging to file: %s (Max:%dMB, Backups:%d, Age:%dd, Compress:%t)\n",
				absPath, cfg.MaxSizeMB, cfg.MaxBackups, cfg.MaxAgeDays, cfg.Compress)
		}

		if !cfg.DisableTerminal {
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
			if !consoleOff {
				fmt.Fprintln(os.Stderr, "Warn: No log outputs configured, defaulting to simple stderr console output.")
				writers = append(writers, zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
			} else {
				fmt.Fprintln(os.Stderr, "Warn: All log outputs disabled (file path empty and terminal disabled). Logging is off.")
				writers = append(writers, io.Discard)
			}
		}

		level, parseErr := ParseLevel(cfg.Level)
		if parseErr != nil {
			fmt.Fprintf(os.Stderr, "Warn: Invalid log level %q: %v. Defaulting to INFO.\n", cfg.Level, parseErr)
			level = zerolog.InfoLevel
		}
		zerolog.SetGlobalLevel(level)
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
		globalL = builder.Logger()
		log.Logger = globalL

		globalL.Info().Msgf("Logger initialized. Global level: %s", level.String())
	})
	return err
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
	if logCloser != nil {
		return logCloser.Close()
	}
	return nil
}

func L() zerolog.Logger {
	if globalL.GetLevel() == zerolog.NoLevel && !consoleOff {
		fmt.Fprintln(os.Stderr, "Warn: logging.L() called before logging.Setup(). Using default stderr logger.")
		_ = Setup(Config{Level: LevelInfo})
	}
	return globalL
}

func isTerminal() bool {
	info, _ := os.Stderr.Stat()
	if info == nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}
