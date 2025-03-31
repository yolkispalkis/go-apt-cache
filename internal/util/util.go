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

	repoNameRegex = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

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

		plainBytes, err := strconv.ParseInt(sizeStr, 10, 64)
		if err == nil && plainBytes >= 0 {
			return plainBytes, nil
		}
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
	if sizeValue > 0 && byteSize <= 0 && multiplier > 1 {

		return 0, fmt.Errorf("size value resulted in non-positive bytes or potential overflow: %q", sizeStr)
	}

	if float64(byteSize)/multiplier < sizeValue*0.99 {
		logging.Warn("Potential precision loss during size conversion", "input", sizeStr, "bytes", byteSize)
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
	cleaned := filepath.Clean(path)

	if filepath.IsAbs(cleaned) || cleaned == "/" || cleaned == `\` {
		return "."
	}
	return cleaned
}

func CleanPathDir(path string) string {
	return filepath.Dir(CleanPath(path))
}

func IsRepoNameSafe(component string) bool {
	if component == "" || component == "." || component == ".." {
		return false
	}

	return repoNameRegex.MatchString(component) && !strings.ContainsAny(component, "/\\")
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
		case ".deb", ".udeb", ".ddeb":
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
		case ".lz4":
			if originalMime != "application/x-lz4" {
				mimeType = "application/x-lz4"
			}
		case ".zst":
			if originalMime != "application/zstd" {
				mimeType = "application/zstd"
			}
		case ".diff", ".patch":
			mimeType = "text/x-diff; charset=utf-8"
		case ".html", ".htm":
			mimeType = "text/html; charset=utf-8"
		case ".txt", ".text", ".log", "":

			switch lowercaseBaseName {
			case "release", "inrelease", "packages", "sources", "translation", "contents":
				mimeType = "text/plain; charset=utf-8"
			default:

				if strings.HasSuffix(lowercaseBaseName, "translation") {
					mimeType = "text/plain; charset=utf-8"
				} else if mimeType == "" || strings.HasPrefix(mimeType, "application/octet-stream") {

				}
			}
		case ".json":
			mimeType = "application/json"
		case ".xml":
			mimeType = "application/xml"

		default:

			if mimeType == "" {
				mimeType = "application/octet-stream"
			}
		}

		if mimeType != originalMime && originalMime != "" && !strings.HasPrefix(originalMime, "application/octet-stream") {
			logging.Debug("MIME type determined by override rule", "path", filePath, "extension", ext, "base_name", baseName, "determined_mime_type", mimeType, "original_mime_type", originalMime)
		}
	}

	if mimeType == "" {
		logging.Warn("Could not determine MIME type, falling back to application/octet-stream", "path", filePath)
		mimeType = "application/octet-stream"
	}

	return mimeType
}

var cacheControlHeaders = map[string]bool{
	"Cache-Control": true,
	"Expires":       true,
	"ETag":          true,
	"Accept-Ranges": true,
}

func SelectCacheControlHeaders(dst, src http.Header) {
	for h, values := range src {
		canonicalH := http.CanonicalHeaderKey(h)
		if cacheControlHeaders[canonicalH] {
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

		}
	}
}

func FormatDuration(d time.Duration) string {
	nanos := d.Nanoseconds()
	sign := ""
	if nanos < 0 {
		sign = "-"
		nanos = -nanos
	}

	switch {
	case nanos == 0:
		return "0s"
	case nanos < int64(time.Microsecond):
		return fmt.Sprintf("%s%dns", sign, nanos)
	case nanos < int64(time.Millisecond):
		return fmt.Sprintf("%s%.3fµs", sign, float64(nanos)/float64(time.Microsecond))
	case nanos < int64(time.Second):
		return fmt.Sprintf("%s%.3fms", sign, float64(nanos)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%s%.3fs", sign, d.Seconds())
	}
}
