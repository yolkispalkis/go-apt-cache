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
	sizeRegex = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGT])?B?$`)

	repoNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

	unsafeCharsRegex = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)

	reservedNames = map[string]struct{}{
		"con": {}, "prn": {}, "aux": {}, "nul": {},
		"com1": {}, "com2": {}, "com3": {}, "com4": {}, "com5": {}, "com6": {}, "com7": {}, "com8": {}, "com9": {},
		"lpt1": {}, "lpt2": {}, "lpt3": {}, "lpt4": {}, "lpt5": {}, "lpt6": {}, "lpt7": {}, "lpt8": {}, "lpt9": {},
	}
)

func ParseSize(sizeStr string) (int64, error) {
	if sizeStr == "" {
		return 0, nil
	}

	matches := sizeRegex.FindStringSubmatch(strings.ToUpper(sizeStr))
	if matches == nil {
		return 0, fmt.Errorf("invalid size format: %q (expected format like '10GB', '500MB', '1024')", sizeStr)
	}

	sizeValue, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid numeric value in size %q: %w", sizeStr, err)
	}

	if sizeValue < 0 {
		return 0, fmt.Errorf("size cannot be negative: %q", sizeStr)
	}

	var multiplier float64 = 1.0
	unit := matches[2]

	switch unit {
	case "K":
		multiplier = 1024
	case "M":
		multiplier = 1024 * 1024
	case "G":
		multiplier = 1024 * 1024 * 1024
	case "T":
		multiplier = 1024 * 1024 * 1024 * 1024
	case "":
		multiplier = 1
	default:
		return 0, fmt.Errorf("unknown size unit %q in %q", unit, sizeStr)
	}

	byteSize := int64(sizeValue * multiplier)

	if sizeValue > 0 && byteSize <= 0 {
		return 0, fmt.Errorf("size value resulted in non-positive bytes or potential overflow: %q", sizeStr)
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
	if path == "" {
		return "."
	}
	return filepath.Clean(path)
}

func CleanPathDir(path string) string {
	return filepath.Dir(CleanPath(path))
}

func IsRepoNameSafe(component string) bool {
	if component == "" || component == "." || component == ".." {
		return false
	}
	return repoNameRegex.MatchString(component)
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
			logging.Warn("Path traversal component detected and removed during sanitization", "component", part, "original_path", path)
			continue
		}

		sanitizedPart := SanitizeFilename(part)

		if sanitizedPart != "" {
			sanitizedParts = append(sanitizedParts, sanitizedPart)
		} else {
			logging.Warn("Path component became empty after sanitization, skipping", "original_component", part, "original_path", path)
		}
	}

	return filepath.Join(sanitizedParts...)
}

func GetContentType(filePath string) string {
	ext := filepath.Ext(filePath)
	baseName := filepath.Base(filePath)

	mimeType := mime.TypeByExtension(ext)

	if mimeType == "" || strings.HasPrefix(mimeType, "application/octet-stream") {
		originalMime := mimeType
		lowercaseExt := strings.ToLower(ext)
		lowercaseBaseName := strings.ToLower(baseName)

		switch lowercaseExt {
		case ".deb", ".udeb":
			mimeType = "application/vnd.debian.binary-package"
		case ".dsc":
			mimeType = "text/plain; charset=utf-8"
		case ".changes":
			mimeType = "text/plain; charset=utf-8"
		case ".gz":
			if originalMime != "application/gzip" && originalMime != "application/x-gzip" {
				mimeType = "application/gzip"
			}
		case ".bz2":
			if originalMime != "application/x-bzip2" {
				mimeType = "application/x-bzip2"
			}
		case ".xz":
			if originalMime != "application/x-xz" {
				mimeType = "application/x-xz"
			}
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
			switch lowercaseBaseName {
			case "release", "inrelease":
				mimeType = "text/plain; charset=utf-8"
			default:
				if mimeType == "" || strings.HasPrefix(mimeType, "application/octet-stream") {
					mimeType = "application/octet-stream"
				}
			}
		default:
			if mimeType == "" {
				mimeType = "application/octet-stream"
			}
		}

		if mimeType != originalMime && mimeType != "application/octet-stream" {
			logging.Debug("MIME type determined by custom rule or override", "path", filePath, "extension", ext, "base_name", baseName, "determined_mime_type", mimeType, "original_mime_type", originalMime)
		}
	}

	if mimeType == "" {
		logging.Warn("Could not determine MIME type, falling back to application/octet-stream", "path", filePath)
		mimeType = "application/octet-stream"
	}

	return mimeType
}

var cacheRelevantHeaders = map[string]bool{
	"Cache-Control": true,
	"Expires":       true,
	"ETag":          true,
	"Accept-Ranges": true,
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
		case "Content-Length",
			"Content-Type",
			"Last-Modified",
			"Date",
			"Connection",
			"Transfer-Encoding",
			"Content-Encoding":
			continue
		}

		if len(values) > 0 {
			dst[canonicalH] = append([]string(nil), values...)
			logging.Debug("Applying header from cache metadata", "header", canonicalH, "value", values)
		}
	}
}

func FormatDuration(d time.Duration) string {
	nanos := d.Nanoseconds()
	if nanos < 0 {
		return fmt.Sprintf("%dns", nanos)
	}

	switch {
	case nanos < 1000:
		return fmt.Sprintf("%dns", nanos)
	case nanos < 1000*1000:
		return fmt.Sprintf("%.3fµs", float64(nanos)/1000.0)
	case nanos < 1000*1000*1000:
		return fmt.Sprintf("%.3fms", float64(nanos)/1000000.0)
	default:
		return fmt.Sprintf("%.3fs", d.Seconds())
	}
}
