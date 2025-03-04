package logging

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/utils"
)

// LogConfig holds configuration for the logger
type LogConfig struct {
	// FilePath is the path to the log file
	FilePath string
	// DisableTerminal disables output to terminal
	DisableTerminal bool
	// MaxSize is the maximum size of the log file (e.g. "10MB", "1GB")
	MaxSize string
	// Level is the minimum log level to output
	Level LogLevel
}

// LogLevel represents the severity of a log message
type LogLevel int

const (
	// DEBUG level for detailed troubleshooting
	DEBUG LogLevel = iota
	// INFO level for general operational information
	INFO
	// WARNING level for potential issues
	WARNING
	// ERROR level for error conditions
	ERROR
	// FATAL level for critical errors that prevent operation
	FATAL
)

// String returns the string representation of the log level
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

// Logger is a custom logger that supports file logging with size limits
type Logger struct {
	config     LogConfig
	mu         sync.Mutex
	file       *os.File
	fileWriter io.Writer
	writers    []io.Writer
	logger     *log.Logger
}

// NewLogger creates a new logger with the given configuration
func NewLogger(config LogConfig) (*Logger, error) {
	logger := &Logger{
		config: config,
	}

	// Initialize writers
	var writers []io.Writer

	// Add terminal writer if enabled
	if !config.DisableTerminal {
		writers = append(writers, os.Stdout)
	}

	// Add file writer if path is specified
	if config.FilePath != "" {
		if err := logger.setupFileWriter(); err != nil {
			return nil, fmt.Errorf("failed to setup file writer: %w", err)
		}
		writers = append(writers, logger.fileWriter)
	}

	// Create multi-writer if we have multiple outputs
	var writer io.Writer
	if len(writers) > 0 {
		writer = io.MultiWriter(writers...)
	} else {
		// If no writers, use a discard writer
		writer = io.Discard
	}

	// Create the standard logger
	logger.logger = log.New(writer, "", log.LstdFlags)
	logger.writers = writers

	return logger, nil
}

// setupFileWriter sets up the file writer with rotation support
func (l *Logger) setupFileWriter() error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(l.config.FilePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	// Open log file
	file, err := os.OpenFile(l.config.FilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	// Parse max size
	maxSize, err := utils.ParseSize(l.config.MaxSize)
	if err != nil {
		// Default to 10MB if parsing fails
		maxSize = 10 * 1024 * 1024
		log.Printf("Warning: Invalid log max size '%s', defaulting to 10MB", l.config.MaxSize)
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

// rotateLogFile rotates the log file when it exceeds the maximum size
func (l *Logger) rotateLogFile() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Close current file
	if l.file != nil {
		l.file.Close()
	}

	// Rename current log file to include timestamp
	backupName := fmt.Sprintf("%s.%s", l.config.FilePath, time.Now().Format("20060102-150405"))
	if err := os.Rename(l.config.FilePath, backupName); err != nil {
		// If rename fails, just try to open a new file
		if !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Failed to rotate log file: %v\n", err)
		}
	}

	// Open new log file
	file, err := os.OpenFile(l.config.FilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open new log file after rotation: %w", err)
	}

	l.file = file

	// Update the file writer
	for i, w := range l.writers {
		if sw, ok := w.(*sizeConstrainedWriter); ok {
			sw.file = file
			sw.currentSize = 0
			l.writers[i] = sw
			l.fileWriter = sw
			break
		}
	}

	// Recreate the logger with the new writer
	var writer io.Writer
	if len(l.writers) > 0 {
		writer = io.MultiWriter(l.writers...)
	} else {
		writer = io.Discard
	}
	l.logger = log.New(writer, "", log.LstdFlags)

	return nil
}

// Close closes the logger and any open files
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

// log logs a message with the given level
func (l *Logger) log(level LogLevel, format string, args ...interface{}) {
	if level < l.config.Level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	prefix := fmt.Sprintf("[%s] ", level.String())
	if format == "" {
		l.logger.Print(prefix)
	} else {
		l.logger.Printf(prefix+format, args...)
	}
}

// Debug logs a debug message
func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(DEBUG, format, args...)
}

// Info logs an info message
func (l *Logger) Info(format string, args ...interface{}) {
	l.log(INFO, format, args...)
}

// Warning logs a warning message
func (l *Logger) Warning(format string, args ...interface{}) {
	l.log(WARNING, format, args...)
}

// Error logs an error message
func (l *Logger) Error(format string, args ...interface{}) {
	l.log(ERROR, format, args...)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(format string, args ...interface{}) {
	l.log(FATAL, format, args...)
	os.Exit(1)
}

// sizeConstrainedWriter is a writer that rotates the file when it exceeds the maximum size
type sizeConstrainedWriter struct {
	file        *os.File
	maxSize     int64
	currentSize int64
	logger      *Logger
}

// Write implements io.Writer
func (w *sizeConstrainedWriter) Write(p []byte) (n int, err error) {
	// Check if we need to rotate
	if w.maxSize > 0 && w.currentSize+int64(len(p)) > w.maxSize {
		if err := w.logger.rotateLogFile(); err != nil {
			return 0, err
		}
		w.currentSize = 0
	}

	// Write to file
	n, err = w.file.Write(p)
	w.currentSize += int64(n)
	return n, err
}

// DefaultLogger is the default logger instance
var DefaultLogger *Logger

// Initialize initializes the default logger
func Initialize(config LogConfig) error {
	logger, err := NewLogger(config)
	if err != nil {
		return err
	}
	DefaultLogger = logger
	return nil
}

// Debug logs a debug message to the default logger
func Debug(format string, args ...interface{}) {
	if DefaultLogger != nil {
		DefaultLogger.Debug(format, args...)
	}
}

// Info logs an info message to the default logger
func Info(format string, args ...interface{}) {
	if DefaultLogger != nil {
		DefaultLogger.Info(format, args...)
	}
}

// Warning logs a warning message to the default logger
func Warning(format string, args ...interface{}) {
	if DefaultLogger != nil {
		DefaultLogger.Warning(format, args...)
	}
}

// Error logs an error message to the default logger
func Error(format string, args ...interface{}) {
	if DefaultLogger != nil {
		DefaultLogger.Error(format, args...)
	}
}

// Fatal logs a fatal message to the default logger and exits
func Fatal(format string, args ...interface{}) {
	if DefaultLogger != nil {
		DefaultLogger.Fatal(format, args...)
	} else {
		log.Fatalf(format, args...)
	}
}

// Close closes the default logger
func Close() error {
	if DefaultLogger != nil {
		return DefaultLogger.Close()
	}
	return nil
}
