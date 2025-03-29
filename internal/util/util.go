// internal/util/util.go
package util

import (
	"fmt"
	"log"
	"mime"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	sizeRegex        = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGT]?B)?$`)
	safePathRegex    = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)
	reservedNames    = map[string]struct{}{"con": {}, "prn": {}, "aux": {}, "nul": {}, "com1": {}, "com2": {}, "com3": {}, "com4": {}, "com5": {}, "com6": {}, "com7": {}, "com8": {}, "com9": {}, "lpt1": {}, "lpt2": {}, "lpt3": {}, "lpt4": {}, "lpt5": {}, "lpt6": {}, "lpt7": {}, "lpt8": {}, "lpt9": {}}
	unsafeCharsRegex = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)
)

func logDebug(format string, args ...interface{}) {
	log.Printf("[DEBUG] "+format, args...)
}

func logWarn(format string, args ...interface{}) {
	log.Printf("[WARN] "+format, args...)
}

func ParseSize(sizeStr string) (int64, error) {
	if sizeStr == "" {
		return 0, nil
	}

	matches := sizeRegex.FindStringSubmatch(strings.ToUpper(sizeStr))
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
	case "B", "": // Bytes
		multiplier = 1
	default:
		return 0, fmt.Errorf("unknown size unit: %s", matches[2])
	}

	if sizeValue < 0 {
		return 0, fmt.Errorf("size cannot be negative: %s", sizeStr)
	}

	return int64(sizeValue * multiplier), nil
}

func FormatSize(sizeBytes int64) string {
	const unit = 1024
	if sizeBytes < unit {
		return fmt.Sprintf("%d B", sizeBytes)
	}
	div, exp := int64(unit), 0
	for n := sizeBytes / unit; n >= unit && exp < 3; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(sizeBytes)/float64(div), "KMGT"[exp])
}

func CleanPath(path string) string {
	return filepath.Clean(path)
}

func CleanPathDir(path string) string {
	return filepath.Dir(CleanPath(path))
}

func IsPathSafe(component string) bool {
	if component == "" || component == "." || component == ".." {
		return false
	}
	return safePathRegex.MatchString(component)
}

func SanitizeFilename(name string) string {
	sanitized := unsafeCharsRegex.ReplaceAllString(name, "_")
	sanitized = strings.Trim(sanitized, ". ")

	if _, reserved := reservedNames[strings.ToLower(sanitized)]; reserved {
		sanitized = "_" + sanitized
	}

	if sanitized == "" {
		return "_"
	}

	return sanitized
}

func SanitizePath(path string) string {
	path = filepath.ToSlash(path)
	parts := strings.Split(path, "/")
	sanitizedParts := make([]string, 0, len(parts))

	for _, part := range parts {
		if part == "" {
			continue
		}
		if part == "." || part == ".." {
			logWarn("Path traversal component detected and removed: %s in path %s", part, path)
			continue
		}
		sanitizedParts = append(sanitizedParts, SanitizeFilename(part))
	}

	return filepath.Join(sanitizedParts...)
}

func GetContentType(filePath string) string {
	ext := filepath.Ext(filePath)
	mimeType := mime.TypeByExtension(ext)
	if mimeType == "" {
		switch ext {
		case ".deb":
			mimeType = "application/vnd.debian.binary-package"
		case ".udeb":
			mimeType = "application/vnd.debian.binary-package"
		case ".dsc":
			mimeType = "text/plain; charset=utf-8"
		case ".changes":
			mimeType = "text/plain; charset=utf-8"
		case ".gz":
			baseExt := filepath.Ext(strings.TrimSuffix(filePath, ext))
			baseMime := mime.TypeByExtension(baseExt)
			if baseMime != "" {
				mimeType = "application/gzip"
			} else {
				mimeType = "application/gzip"
			}
		case ".bz2":
			mimeType = "application/x-bzip2"
		case ".xz":
			mimeType = "application/x-xz"
		case "":
			mimeType = "application/octet-stream"
		default:
			mimeType = "application/octet-stream"
		}
		logDebug("MIME type for extension %q (path %s) not found in system DB, using fallback: %s", ext, filePath, mimeType)
	}
	return mimeType
}

// CopyRelevantHeaders copies headers relevant for caching/client responses.
func CopyRelevantHeaders(dst, src http.Header) {
	headersToCopy := []string{
		"Content-Length",
		"Content-Type",
		"Date",
		"Expires",
		"Cache-Control",
		"Last-Modified",
		"ETag",
	}
	for _, h := range headersToCopy {
		if values := src.Values(h); len(values) > 0 {
			dst.Set(h, values[0])
			for i := 1; i < len(values); i++ {
				dst.Add(h, values[i])
			}
		}
	}
}

// FormatDuration formats duration for logging/display.
func FormatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.3fms", float64(d.Microseconds())/1000.0)
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Milliseconds()))
	}
	return fmt.Sprintf("%.3fs", d.Seconds())
}
