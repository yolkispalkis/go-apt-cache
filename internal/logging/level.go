package logging

import "strings"

func ParseLogLevel(level string) LogLevel {
	switch strings.ToLower(level) {
	case "debug":
		return DEBUG
	case "info":
		return INFO
	case "warning", "warn":
		return WARNING
	case "error":
		return ERROR
	case "fatal":
		return FATAL
	default:
		return INFO
	}
}
