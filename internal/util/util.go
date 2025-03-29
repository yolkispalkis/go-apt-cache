// internal/util/util.go
package util

import (
	"fmt"
	"mime"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/logging"
)

var (
	sizeRegex        = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGT]?B)?$`)
	safePathRegex    = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)
	reservedNames    = map[string]struct{}{"con": {}, "prn": {}, "aux": {}, "nul": {}, "com1": {}, "com2": {}, "com3": {}, "com4": {}, "com5": {}, "com6": {}, "com7": {}, "com8": {}, "com9": {}, "lpt1": {}, "lpt2": {}, "lpt3": {}, "lpt4": {}, "lpt5": {}, "lpt6": {}, "lpt7": {}, "lpt8": {}, "lpt9": {}}
	unsafeCharsRegex = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`) // Control chars + windows forbidden
)

// ParseSize converts human-readable size string (e.g., "10GB") to bytes.
func ParseSize(sizeStr string) (int64, error) {
	if sizeStr == "" {
		return 0, nil // Or return error? Depends on context.
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

// FormatSize converts bytes to human-readable string.
func FormatSize(sizeBytes int64) string {
	const unit = 1024
	if sizeBytes < unit {
		return fmt.Sprintf("%d B", sizeBytes)
	}
	div, exp := int64(unit), 0
	for n := sizeBytes / unit; n >= unit && exp < 3; n /= unit { // Stop at TB
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(sizeBytes)/float64(div), "KMGT"[exp])
}

// CleanPath cleans and canonicalizes a file path.
// It's crucial for security to prevent path traversal.
func CleanPath(path string) string {
	return filepath.Clean(path)
}

// CleanPathDir cleans the directory part of a path.
func CleanPathDir(path string) string {
	return filepath.Dir(CleanPath(path))
}

// IsPathSafe checks if a single path component is safe for filesystem/URL use.
func IsPathSafe(component string) bool {
	if component == "" || component == "." || component == ".." {
		return false
	}
	// Basic check for common safe characters. Adjust regex as needed.
	return safePathRegex.MatchString(component)
	// Could add checks for reserved names on Windows if targeting it specifically.
}

// SanitizeFilename replaces unsafe characters in a single filename component.
func SanitizeFilename(name string) string {
	// Replace unsafe characters with underscore (or other safe char)
	sanitized := unsafeCharsRegex.ReplaceAllString(name, "_")

	// Trim leading/trailing dots and spaces (problematic on Windows)
	sanitized = strings.Trim(sanitized, ". ")

	// Check for reserved names (case-insensitive on Windows)
	if _, reserved := reservedNames[strings.ToLower(sanitized)]; reserved {
		sanitized = "_" + sanitized // Prepend underscore
	}

	// Ensure filename is not empty after sanitization
	if sanitized == "" {
		return "_" // Default to underscore if empty
	}

	// Limit length? (OS-dependent) - Skip for now

	return sanitized
}

// SanitizePath cleans and sanitizes all components of a relative path.
func SanitizePath(path string) string {
	// Use forward slashes consistently
	path = filepath.ToSlash(path)
	// Split into components
	parts := strings.Split(path, "/")
	sanitizedParts := make([]string, 0, len(parts))

	for _, part := range parts {
		// Skip empty parts resulting from multiple slashes (e.g., "a//b")
		if part == "" {
			continue
		}
		// VERY IMPORTANT: Prevent path traversal explicitly
		if part == "." || part == ".." {
			// Disallow traversal components entirely
			logging.Warn("Path traversal component detected and removed: %s in path %s", part, path)
			continue
		}
		sanitizedParts = append(sanitizedParts, SanitizeFilename(part))
	}

	// Join back using OS-specific separator for filesystem operations
	return filepath.Join(sanitizedParts...)
}

// GetContentType determines the MIME type based on file extension.
func GetContentType(filePath string) string {
	ext := filepath.Ext(filePath)
	mimeType := mime.TypeByExtension(ext)
	if mimeType == "" {
		// Provide fallbacks for common APT types not always in system mime.types
		switch ext {
		case ".deb":
			mimeType = "application/vnd.debian.binary-package"
		case ".udeb":
			mimeType = "application/vnd.debian.binary-package"
		case ".dsc":
			mimeType = "text/plain; charset=utf-8" // Often text
		case ".changes":
			mimeType = "text/plain; charset=utf-8" // Often text
		case ".gz":
			// Handle cases like Packages.gz - base type is needed
			baseExt := filepath.Ext(strings.TrimSuffix(filePath, ext))
			baseMime := mime.TypeByExtension(baseExt)
			if baseMime != "" {
				// Technically incorrect (should be application/gzip), but often
				// what clients expect if they don't handle Content-Encoding.
				// Sticking to application/gzip is more correct.
				mimeType = "application/gzip"
			} else {
				mimeType = "application/gzip"
			}
		case ".bz2":
			mimeType = "application/x-bzip2"
		case ".xz":
			mimeType = "application/x-xz"
		case "": // No extension
			// Could try http.DetectContentType on first 512 bytes if content is available
			mimeType = "application/octet-stream" // Default binary
		default:
			mimeType = "application/octet-stream" // Default fallback
		}
		logging.Debug("MIME type for extension %q (path %s) not found in system DB, using fallback: %s", ext, filePath, mimeType)
	}
	return mimeType
}

// CopyRelevantHeaders copies headers relevant for caching/client responses.
func CopyRelevantHeaders(dst, src http.Header) {
	// Headers relevant for client understanding and caching proxies
	headersToCopy := []string{
		"Content-Length", // Usually set explicitly based on actual size later
		"Content-Type",   // Usually set explicitly based on extension later
		"Date",
		"Expires",
		"Cache-Control",
		"Last-Modified",
		"ETag",
		// Add others like Content-Encoding if handling compression
	}
	for _, h := range headersToCopy {
		if values := src.Values(h); len(values) > 0 {
			// Use Set to replace, Add to append
			dst.Set(h, values[0])
			for i := 1; i < len(values); i++ {
				dst.Add(h, values[i])
			}
		}
	}
}

// FormatDuration formats duration for logging/display.
func FormatDuration(d time.Duration) string {
	// Provide slightly more readable format than default String()
	if d < time.Millisecond {
		return fmt.Sprintf("%.3fms", float64(d.Microseconds())/1000.0)
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Milliseconds()))
	}
	return fmt.Sprintf("%.3fs", d.Seconds())
}
