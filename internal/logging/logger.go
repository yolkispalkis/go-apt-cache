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
	logSync func() error = func() error { return nil }
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

	level, err := ParseLevel(cfg.Level)
	if err != nil {
		fmt.Printf("Warning: Invalid log level %q in config during setup: %v. Defaulting to INFO.\n", cfg.Level, err)
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	multi := zerolog.MultiLevelWriter(writers...)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	log.Logger = zerolog.New(multi).With().Timestamp().Caller().Logger()
	log.Info().Msgf("Logger initialized with level: %s", level.String())

	return nil
}

func ParseLevel(levelStr string) (zerolog.Level, error) {
	switch strings.ToLower(levelStr) {
	case LevelDebug:
		return zerolog.DebugLevel, nil
	case LevelInfo:
		return zerolog.InfoLevel, nil
	case LevelWarn, "warning":
		return zerolog.WarnLevel, nil
	case LevelError:
		return zerolog.ErrorLevel, nil
	default:
		return zerolog.InfoLevel, fmt.Errorf("unknown log level %q (valid levels: debug, info, warn, error)", levelStr)
	}
}

func Sync() error {
	return logSync()
}

func argsToMap(args []any) map[string]interface{} {
	fields := make(map[string]interface{})
	if len(args)%2 != 0 {
		log.Warn().Msg("Odd number of arguments provided for key-value logging, ignoring last argument.")
		args = args[:len(args)-1]
	}
	for i := 0; i < len(args); i += 2 {
		key, keyOk := args[i].(string)
		if !keyOk {
			log.Warn().Interface("invalid_key_type_arg", args[i]).Int("arg_index", i).Msg("Non-string key provided for key-value logging, skipping pair.")
			continue
		}
		fields[key] = args[i+1]
	}
	return fields
}

func Debug(msg string, args ...any) {
	log.Debug().Fields(argsToMap(args)).Msg(msg)
}

func Info(msg string, args ...any) {
	log.Info().Fields(argsToMap(args)).Msg(msg)
}

func Warn(msg string, args ...any) {
	log.Warn().Fields(argsToMap(args)).Msg(msg)
}

func Error(msg string, args ...any) {
	log.Error().Fields(argsToMap(args)).Msg(msg)
}

func ErrorE(msg string, err error, args ...any) {
	log.Error().Err(err).Fields(argsToMap(args)).Msg(msg)
}
