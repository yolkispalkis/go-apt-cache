package utils

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
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
