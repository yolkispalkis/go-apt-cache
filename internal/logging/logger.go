package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	logSync func() error
)

const (
	LevelDebug = "debug"
	LevelInfo  = "info"
	LevelWarn  = "warn"
	LevelError = "error"
)

type LoggingConfig struct {
	Level           string
	FilePath        string
	DisableTerminal bool
	MaxSizeMB       int
	MaxBackups      int
	MaxAgeDays      int
	Compress        bool
}

func Setup(cfg LoggingConfig) error {
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

	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		short := file

		projectName := "go-apt-cache/"
		idx := strings.Index(file, projectName)
		if idx != -1 {

			short = file[idx+len(projectName):]
		} else {

			dir := filepath.Dir(file)
			base := filepath.Base(file)
			grandDir := filepath.Base(dir)
			if grandDir != "." && grandDir != "/" && grandDir != "" {
				short = filepath.Join(grandDir, base)
			} else {
				short = base
			}
		}
		return fmt.Sprintf("%s:%d", short, line)
	}

	if !cfg.DisableTerminal {
		consoleWriter := zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
			NoColor:    false,

			FormatCaller: func(i interface{}) string {
				if s, ok := i.(string); ok {

					return "[" + s + "]"
				}
				return fmt.Sprintf("[%v]", i)
			},
		}
		writers = append(writers, consoleWriter)
	}

	if len(writers) == 0 {
		writers = append(writers, io.Discard)
		fmt.Println("Warning: No log outputs configured (file or terminal), logging is effectively disabled.")
	}

	level := parseLevel(cfg.Level)
	zerolog.SetGlobalLevel(level)

	multi := zerolog.MultiLevelWriter(writers...)

	zerolog.TimeFieldFormat = time.RFC3339

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

func Sync() error {
	if logSync != nil {
		return logSync()
	}
	return nil
}

func argsToMap(args []any) map[string]interface{} {
	fields := make(map[string]interface{})
	for i := 0; i < len(args); {
		key, keyOk := args[i].(string)
		if !keyOk || i+1 >= len(args) {

			i++
			continue
		}
		fields[key] = args[i+1]
		i += 2
	}
	return fields
}

func Debug(msg string, args ...any) {
	event := log.Debug()
	if len(args) > 0 {
		if strings.Contains(msg, "%") {
			event.Msgf(msg, args...)
		} else {
			event.Fields(argsToMap(args)).Msg(msg)
		}
	} else {
		event.Msg(msg)
	}
}

func Info(msg string, args ...any) {
	event := log.Info()
	if len(args) > 0 {
		if strings.Contains(msg, "%") {
			event.Msgf(msg, args...)
		} else {
			event.Fields(argsToMap(args)).Msg(msg)
		}
	} else {
		event.Msg(msg)
	}
}

func Warn(msg string, args ...any) {
	event := log.Warn()
	if len(args) > 0 {
		if strings.Contains(msg, "%") {
			event.Msgf(msg, args...)
		} else {
			event.Fields(argsToMap(args)).Msg(msg)
		}
	} else {
		event.Msg(msg)
	}
}

func Error(msg string, args ...any) {
	event := log.Error()
	if len(args) > 0 {
		if strings.Contains(msg, "%") {
			event.Msgf(msg, args...)
		} else {
			event.Fields(argsToMap(args)).Msg(msg)
		}
	} else {
		event.Msg(msg)
	}
}

func ErrorE(msg string, err error, args ...any) {
	event := log.Error().Err(err)
	if len(args) > 0 {
		if strings.Contains(msg, "%") {
			event.Msgf(msg, args...)
		} else {
			event.Fields(argsToMap(args)).Msg(msg)
		}
	} else {
		event.Msg(msg)
	}
}
