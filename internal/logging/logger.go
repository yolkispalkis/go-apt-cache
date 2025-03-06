package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

func ParseSize(sizeStr string) (int64, error) {
	if sizeStr == "" {
		return 0, nil
	}

	re := regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGT]?B)?$`)
	matches := re.FindStringSubmatch(strings.ToUpper(sizeStr))

	if matches == nil {
		return 0, fmt.Errorf("invalid size format: %s", sizeStr)
	}

	sizeValue, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size value: %s", matches[1])
	}

	var multiplier float64 = 1
	switch matches[2] {
	case "KB", "K":
		multiplier = 1024
	case "MB", "M":
		multiplier = 1024 * 1024
	case "GB", "G":
		multiplier = 1024 * 1024 * 1024
	case "TB", "T":
		multiplier = 1024 * 1024 * 1024 * 1024
	case "B", "":
	default:
		return 0, fmt.Errorf("unknown size unit: %s", matches[2])
	}

	return int64(sizeValue * multiplier), nil
}

type LogConfig struct {
	FilePath        string
	DisableTerminal bool
	MaxSize         string
	Level           LogLevel
}

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARNING
	ERROR
	FATAL
)

func (l LogLevel) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARNING:
		return "WARN"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

type Logger struct {
	config     LogConfig
	mu         sync.Mutex
	file       *os.File
	fileWriter io.Writer
	writers    []io.Writer
	logger     *loggerImpl
}

type loggerImpl struct {
	out io.Writer
	mu  sync.Mutex
}

func (l *loggerImpl) Print(v ...interface{}) {
	l.Output(2, fmt.Sprint(v...))
}

func (l *loggerImpl) Printf(format string, v ...interface{}) {
	l.Output(2, fmt.Sprintf(format, v...))
}

func (l *loggerImpl) Output(calldepth int, s string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, err := l.out.Write([]byte(s + "\n"))
	return err
}

func NewLogger(config LogConfig) (*Logger, error) {
	logger := &Logger{
		config: config,
	}

	var writers []io.Writer

	if !config.DisableTerminal {
		writers = append(writers, os.Stdout)
	}

	if config.FilePath != "" {
		if err := logger.setupFileWriter(); err != nil {
			return nil, fmt.Errorf("failed to setup file writer: %w", err)
		}
		writers = append(writers, logger.fileWriter)
	}

	var writer io.Writer
	if len(writers) > 0 {
		writer = io.MultiWriter(writers...)
	} else {
		writer = io.Discard
	}

	logger.logger = &loggerImpl{out: writer}
	logger.writers = writers

	return logger, nil
}

func (l *Logger) setupFileWriter() error {
	dir := filepath.Dir(l.config.FilePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	file, err := os.OpenFile(l.config.FilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	maxSize, err := ParseSize(l.config.MaxSize)
	if err != nil {
		maxSize = 10 * 1024 * 1024
		Warning("Invalid log max size '%s', defaulting to 10MB", l.config.MaxSize)
	}

	l.file = file
	l.fileWriter = &sizeConstrainedWriter{
		file:        file,
		maxSize:     maxSize,
		currentSize: 0,
		logger:      l,
	}

	return nil
}

func (l *Logger) rotateLogFile() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file != nil {
		l.file.Close()
	}

	backupName := fmt.Sprintf("%s.%s", l.config.FilePath, time.Now().Format("20060102-150405"))
	if err := os.Rename(l.config.FilePath, backupName); err != nil {
		if !os.IsNotExist(err) {
			Error("Failed to rotate log file: %v", err)
		}
	}

	file, err := os.OpenFile(l.config.FilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open new log file after rotation: %w", err)
	}

	l.file = file

	for i, w := range l.writers {
		if sw, ok := w.(*sizeConstrainedWriter); ok {
			sw.file = file
			sw.currentSize = 0
			l.writers[i] = sw
			l.fileWriter = sw
			break
		}
	}

	var writer io.Writer
	if len(l.writers) > 0 {
		writer = io.MultiWriter(l.writers...)
	} else {
		writer = io.Discard
	}
	l.logger = &loggerImpl{out: writer}

	return nil
}

func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

func (l *Logger) log(level LogLevel, format string, args ...interface{}) {
	if level < l.config.Level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	prefix := fmt.Sprintf("[%s] ", level.String())
	var message string
	if format == "" {
		message = fmt.Sprint(args...)
	} else {
		message = fmt.Sprintf(format, args...)
	}
	l.logger.Output(2, prefix+message)
}

func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(DEBUG, format, args...)
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.log(INFO, format, args...)
}

func (l *Logger) Warning(format string, args ...interface{}) {
	l.log(WARNING, format, args...)
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.log(ERROR, format, args...)
}

func (l *Logger) Fatal(format string, args ...interface{}) {
	l.log(FATAL, format, args...)
	os.Exit(1)
}

type sizeConstrainedWriter struct {
	file        *os.File
	maxSize     int64
	currentSize int64
	logger      *Logger
}

func (w *sizeConstrainedWriter) Write(p []byte) (n int, err error) {
	if w.maxSize > 0 && w.currentSize+int64(len(p)) > w.maxSize {
		if err := w.logger.rotateLogFile(); err != nil {
			return 0, err
		}
		w.currentSize = 0
	}

	n, err = w.file.Write(p)
	w.currentSize += int64(n)
	return n, err
}

var DefaultLogger *Logger

func Initialize(config LogConfig) error {
	logger, err := NewLogger(config)
	if err != nil {
		return err
	}
	DefaultLogger = logger
	return nil
}

func Debug(format string, args ...interface{}) {
	if DefaultLogger != nil {
		DefaultLogger.Debug(format, args...)
	}
}

func Info(format string, args ...interface{}) {
	if DefaultLogger != nil {
		DefaultLogger.Info(format, args...)
	}
}

func Warning(format string, args ...interface{}) {
	if DefaultLogger != nil {
		DefaultLogger.Warning(format, args...)
	}
}

func Error(format string, args ...interface{}) {
	if DefaultLogger != nil {
		DefaultLogger.Error(format, args...)
	}
}

func Fatal(format string, args ...interface{}) {
	if DefaultLogger != nil {
		DefaultLogger.Fatal(format, args...)
	} else {
		fmt.Printf("FATAL: "+format+"\n", args...)
		os.Exit(1)
	}
}

func Close() error {
	if DefaultLogger != nil {
		return DefaultLogger.Close()
	}
	return nil
}
