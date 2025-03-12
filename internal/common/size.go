package common

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// ParseSize converts a human-readable size string to an integer number of bytes.
func ParseSize(sizeStr string) (int64, error) {
	if sizeStr == "" {
		return 0, nil
	}

	re := regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)\s*([KMGT]?B)?$`)
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
	case "B", "BYTES", "":
		// multiplier remains 1
	default:
		return 0, fmt.Errorf("unknown size unit: %s", matches[2])
	}

	return int64(sizeValue * multiplier), nil
}

// FormatSize returns a human-readable string representation of size in bytes.
func FormatSize(sizeBytes int64) string {
	const unit = 1024
	if sizeBytes < unit {
		return fmt.Sprintf("%d B", sizeBytes)
	}
	d, exp := int64(unit), 0
	for n := sizeBytes / unit; n >= unit; n /= unit {
		d *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(sizeBytes)/float64(d), "KMGT"[exp])
}

// ConvertSizeWithUnit converts a given size with unit to bytes.
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
		return size
	default:
		return size
	}
}
