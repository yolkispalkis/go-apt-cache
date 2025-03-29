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
	sizeRegex = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGT]?B)?$`)

	safePathRegex = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

	reservedNames = map[string]struct{}{
		"con": {}, "prn": {}, "aux": {}, "nul": {},
		"com1": {}, "com2": {}, "com3": {}, "com4": {}, "com5": {}, "com6": {}, "com7": {}, "com8": {}, "com9": {},
		"lpt1": {}, "lpt2": {}, "lpt3": {}, "lpt4": {}, "lpt5": {}, "lpt6": {}, "lpt7": {}, "lpt8": {}, "lpt9": {},
	}

	unsafeCharsRegex = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)
)

func ParseSize(sizeStr string) (int64, error) {
	if sizeStr == "" {
		return 0, nil
	}

	matches := sizeRegex.FindStringSubmatch(strings.ToUpper(sizeStr))
	if matches == nil {
		return 0, fmt.Errorf("invalid size format: %q", sizeStr)
	}

	sizeValue, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {

		return 0, fmt.Errorf("invalid size value in %q: %w", sizeStr, err)
	}

	var multiplier float64 = 1.0
	unit := matches[2]
	switch unit {
	case "KB", "K":
		multiplier = 1024
	case "MB", "M":
		multiplier = 1024 * 1024
	case "GB", "G":
		multiplier = 1024 * 1024 * 1024
	case "TB", "T":
		multiplier = 1024 * 1024 * 1024 * 1024
	case "B", "":
		multiplier = 1
	default:

		return 0, fmt.Errorf("unknown size unit %q in %q", unit, sizeStr)
	}

	if sizeValue < 0 {
		return 0, fmt.Errorf("size cannot be negative: %q", sizeStr)
	}

	byteSize := int64(sizeValue * multiplier)

	if sizeValue > 0 && byteSize <= 0 {
		return 0, fmt.Errorf("size value resulted in overflow: %q", sizeStr)
	}

	return byteSize, nil
}

func FormatSize(sizeBytes int64) string {
	const unit = 1024
	if sizeBytes < 0 {
		return fmt.Sprintf("%d B", sizeBytes)
	}
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

	if _, isReserved := reservedNames[strings.ToLower(sanitized)]; isReserved {
		sanitized = "_" + sanitized
	}

	if sanitized == "" {
		return "_"
	}

	return sanitized
}

func SanitizePath(path string) string {

	cleaned := filepath.ToSlash(path)
	parts := strings.Split(cleaned, "/")
	sanitizedParts := make([]string, 0, len(parts))

	for _, part := range parts {

		if part == "" {
			continue
		}

		if part == "." || part == ".." {
			logging.Warn("Path traversal component detected and removed: %q in path %q", part, path)
			continue
		}

		sanitizedPart := SanitizeFilename(part)

		if sanitizedPart != "" {
			sanitizedParts = append(sanitizedParts, sanitizedPart)
		} else {
			logging.Warn("Path component %q became empty after sanitization in path %q", part, path)
		}
	}

	return filepath.Join(sanitizedParts...)
}

func GetContentType(filePath string) string {
	ext := filepath.Ext(filePath)

	mimeType := mime.TypeByExtension(ext)

	if mimeType == "" || strings.HasPrefix(mimeType, "application/octet-stream") {

		switch strings.ToLower(ext) {
		case ".deb", ".udeb":
			mimeType = "application/vnd.debian.binary-package"
		case ".dsc":
			mimeType = "text/plain; charset=utf-8"
		case ".changes":
			mimeType = "text/plain; charset=utf-8"
		case ".gz":

			baseExt := filepath.Ext(strings.TrimSuffix(filePath, ext))
			baseMime := mime.TypeByExtension(baseExt)
			if baseMime != "" && !strings.HasPrefix(baseMime, "application/octet-stream") {

				mimeType = "application/gzip"
			} else {
				mimeType = "application/gzip"
			}
		case ".bz2":
			mimeType = "application/x-bzip2"
		case ".xz":
			mimeType = "application/x-xz"
		case ".diff", ".patch":
			mimeType = "text/x-diff; charset=utf-8"
		case ".html", ".htm":
			mimeType = "text/html; charset=utf-8"
		case ".txt", ".text", ".log":
			mimeType = "text/plain; charset=utf-8"
		case ".json":
			mimeType = "application/json"
		case ".xml":
			mimeType = "application/xml"
		case "":

			mimeType = "application/octet-stream"
		default:

			if mimeType == "" {
				mimeType = "application/octet-stream"
			}
		}
		if mimeType != "application/octet-stream" {
			logging.Debug("MIME type for extension %q (path %q) determined by custom rule: %s", ext, filePath, mimeType)
		}
	}
	return mimeType
}

var cacheRelevantHeaders = map[string]bool{
	"Cache-Control": true,
	"Expires":       true,
	"ETag":          true,
	"Accept-Ranges": true,
	"Content-Type":  true,
}

func SelectCacheHeaders(dst, src http.Header) {
	for h, values := range src {
		canonicalH := http.CanonicalHeaderKey(h)
		if cacheRelevantHeaders[canonicalH] {
			if len(values) > 0 {

				dst[canonicalH] = append([]string(nil), values...)
			}
		}
	}
}

func ApplyCacheHeaders(dst http.Header, srcCacheMetaHeaders http.Header) {
	for h, values := range srcCacheMetaHeaders {
		canonicalH := http.CanonicalHeaderKey(h)

		switch canonicalH {
		case "Content-Length", "Content-Type", "Last-Modified", "Date", "Connection", "Transfer-Encoding":
			continue
		}

		if len(values) > 0 {

			dst[canonicalH] = append([]string(nil), values...)
			logging.Debug("Applying header from cache metadata: %s: %v", canonicalH, values)
		}
	}
}

func FormatDuration(d time.Duration) string {
	if d < time.Millisecond {

		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	if d < time.Second {

		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	}

	return fmt.Sprintf("%.3fs", d.Seconds())
}
