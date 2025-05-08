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

type LoggingConfig struct {
	Level           string `json:"level"`
	FilePath        string `json:"filePath"`
	DisableTerminal bool   `json:"disableTerminal"`
	MaxSizeMB       int    `json:"maxSizeMB"`
	MaxBackups      int    `json:"maxBackups"`
	MaxAgeDays      int    `json:"maxAgeDays"`
	Compress        bool   `json:"compress"`
}

func DefaultConfig() LoggingConfig {
	return LoggingConfig{
		Level:           LevelInfo,
		FilePath:        "",
		DisableTerminal: false,
		MaxSizeMB:       100,
		MaxBackups:      5,
		MaxAgeDays:      30,
		Compress:        true,
	}
}

var (
	globalLogger    zerolog.Logger
	logCloser       io.Closer
	setupOnce       sync.Once
	consoleExcluded bool
)

func Setup(cfg LoggingConfig) error {
	var err error
	setupOnce.Do(func() {
		writers := make([]io.Writer, 0, 2)
		consoleExcluded = cfg.DisableTerminal

		if cfg.FilePath != "" {
			absLogPath, pathErr := filepath.Abs(cfg.FilePath)
			if pathErr != nil {
				err = fmt.Errorf("failed to determine absolute log path for '%s': %w", cfg.FilePath, pathErr)
				return
			}
			logDir := filepath.Dir(absLogPath)
			if dirErr := os.MkdirAll(logDir, 0750); dirErr != nil {
				err = fmt.Errorf("failed to create log directory '%s': %w", logDir, dirErr)
				return
			}

			lj := &lumberjack.Logger{
				Filename:   absLogPath,
				MaxSize:    cfg.MaxSizeMB,
				MaxBackups: cfg.MaxBackups,
				MaxAge:     cfg.MaxAgeDays,
				Compress:   cfg.Compress,
				LocalTime:  true,
			}
			writers = append(writers, lj)
			logCloser = lj
			fmt.Fprintf(os.Stderr, "Logging to file: %s (Max Size: %dMB, Max Backups: %d, Max Age: %d days, Compress: %t)\n",
				absLogPath, cfg.MaxSizeMB, cfg.MaxBackups, cfg.MaxAgeDays, cfg.Compress)
		}

		if !cfg.DisableTerminal {
			consoleWriter := zerolog.ConsoleWriter{
				Out:        os.Stderr,
				TimeFormat: time.RFC3339,
				NoColor:    false,
				FormatLevel: func(i interface{}) string {
					if level, ok := i.(string); ok {
						return strings.ToUpper(fmt.Sprintf("[%s]", level))
					}
					return fmt.Sprintf("[%v]", i)
				},
				FormatCaller: func(i interface{}) string {
					if s, ok := i.(string); ok && s != "" {
						return fmt.Sprintf("<%s>", s)
					}
					return ""
				},
			}

			if !isTerminal(os.Stderr.Fd()) {
				consoleWriter.NoColor = true
			}
			writers = append(writers, consoleWriter)
		}

		if len(writers) == 0 {

			if !consoleExcluded {
				fmt.Fprintln(os.Stderr, "Warning: No log outputs configured, defaulting to simple stderr console output.")
				writers = append(writers, zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
			} else {
				fmt.Fprintln(os.Stderr, "Warning: All log outputs disabled (file path empty and terminal disabled). Logging is off.")
				writers = append(writers, io.Discard)
			}
		}

		level, parseErr := ParseLevel(cfg.Level)
		if parseErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: Invalid log level %q: %v. Defaulting to INFO.\n", cfg.Level, parseErr)
			level = zerolog.InfoLevel
		}
		zerolog.SetGlobalLevel(level)

		zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
			short := file

			markers := []string{"/go-apt-cache/", "/internal/", "/pkg/"}
			foundMarker := false
			for _, marker := range markers {
				if idx := strings.LastIndex(file, marker); idx != -1 {
					short = file[idx+1:]
					foundMarker = true
					break
				}
			}
			if !foundMarker {
				short = filepath.Base(file)
			}
			return fmt.Sprintf("%s:%d", short, line)
		}

		multiWriter := zerolog.MultiLevelWriter(writers...)
		zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs

		loggerBuilder := zerolog.New(multiWriter).With().Timestamp()

		if level <= zerolog.DebugLevel {
			loggerBuilder = loggerBuilder.Caller()
		}
		globalLogger = loggerBuilder.Logger()
		log.Logger = globalLogger

		globalLogger.Info().Msgf("Logger initialized. Global level: %s", level.String())
	})
	return err
}

func ParseLevel(levelStr string) (zerolog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(levelStr)) {
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
		return zerolog.NoLevel, fmt.Errorf("unknown log level: %q (valid: debug, info, warn, error, fatal, panic)", levelStr)
	}
}

func Sync() error {
	if logCloser != nil {
		return logCloser.Close()
	}
	return nil
}

func Get() zerolog.Logger {
	if globalLogger.GetLevel() == zerolog.NoLevel && !consoleExcluded {

		fmt.Fprintln(os.Stderr, "Warning: logging.Get() called before logging.Setup(). Using default stderr logger.")
		cfg := DefaultConfig()
		_ = Setup(cfg)
	}
	return globalLogger
}

func isTerminal(fd uintptr) bool {
	fileInfo, _ := os.Stdout.Stat()
	return (fileInfo.Mode() & os.ModeCharDevice) != 0
}

func Info() *zerolog.Event  { return globalLogger.Info() }
func Debug() *zerolog.Event { return globalLogger.Debug() }
func Warn() *zerolog.Event  { return globalLogger.Warn() }
func Error() *zerolog.Event { return globalLogger.Error() }
func Fatal() *zerolog.Event { return globalLogger.Fatal() }
func Panic() *zerolog.Event { return globalLogger.Panic() }
