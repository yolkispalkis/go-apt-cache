package utils

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// ParseSize parses a size string like "10MB" or "2GB" and returns the size in bytes
// Supported units: B, KB, MB, GB, TB (case insensitive)
// If no unit is specified, bytes are assumed
func ParseSize(sizeStr string) (int64, error) {
	// If the input is empty, return 0
	if sizeStr == "" {
		return 0, nil
	}

	// Regular expression to match a number followed by an optional unit
	re := regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGT]?B)?$`)
	matches := re.FindStringSubmatch(strings.ToUpper(sizeStr))

	if matches == nil {
		return 0, fmt.Errorf("invalid size format: %s", sizeStr)
	}

	// Parse the number part
	sizeValue, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size value: %s", matches[1])
	}

	// Convert to bytes based on the unit
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
		// Already in bytes
	default:
		return 0, fmt.Errorf("unknown size unit: %s", matches[2])
	}

	return int64(sizeValue * multiplier), nil
}

// FormatSize formats a size in bytes to a human-readable string
// e.g. 1024 -> "1KB", 1048576 -> "1MB"
func FormatSize(sizeBytes int64) string {
	const unit = 1024
	if sizeBytes < unit {
		return fmt.Sprintf("%d B", sizeBytes)
	}
	div, exp := int64(unit), 0
	for n := sizeBytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(sizeBytes)/float64(div), "KMGT"[exp])
}

// ConvertSizeWithUnit converts a size value from a specified unit to bytes
// This is different from ParseSize as it takes separate size and unit parameters
func ConvertSizeWithUnit(size int64, unit string) int64 {
	switch strings.ToUpper(unit) {
	case "KB", "K":
		return size * 1024
	case "MB", "M":
		return size * 1024 * 1024
	case "GB", "G":
		return size * 1024 * 1024 * 1024
	case "TB", "T":
		return size * 1024 * 1024 * 1024 * 1024
	case "B", "BYTES", "":
		// No conversion needed
		return size
	default:
		// Log warning and return original size
		return size
	}
}
