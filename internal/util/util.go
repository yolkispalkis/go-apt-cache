package util

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	sizeRegex     = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGT])?B?$`)
	repoNameRegex = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

	fileNameUnsafeCharsRegex = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F\s]+`)

	reservedNames = map[string]struct{}{
		"con": {}, "prn": {}, "aux": {}, "nul": {},
		"com1": {}, "com2": {}, "com3": {}, "com4": {}, "com5": {}, "com6": {}, "com7": {}, "com8": {}, "com9": {},
		"lpt1": {}, "lpt2": {}, "lpt3": {}, "lpt4": {}, "lpt5": {}, "lpt6": {}, "lpt7": {}, "lpt8": {}, "lpt9": {},
	}
)

func ParseSize(sizeStr string) (int64, error) {
	if sizeStr == "" {
		return 0, errors.New("size string cannot be empty")
	}

	matches := sizeRegex.FindStringSubmatch(strings.ToUpper(strings.TrimSpace(sizeStr)))
	if matches == nil {

		plainBytes, err := strconv.ParseInt(sizeStr, 10, 64)
		if err == nil && plainBytes >= 0 {
			return plainBytes, nil
		}
		return 0, fmt.Errorf("invalid size format: %q (expected e.g., '10GB', '500MB', '1024')", sizeStr)
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

	if sizeValue > 0 && byteSize <= 0 && multiplier > 1.0 {
		return 0, fmt.Errorf("size value %q resulted in non-positive bytes or overflow", sizeStr)
	}
	return byteSize, nil
}

func FormatSize(sizeBytes int64) string {
	if sizeBytes < 0 {
		return fmt.Sprintf("%d B (Negative)", sizeBytes)
	}
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
	if path == "" {
		return "."
	}
	return filepath.Clean(path)
}

func IsRepoNameSafe(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, "/\\") {
		return false
	}
	return repoNameRegex.MatchString(name)
}

func SanitizeCacheKeyPathComponent(name string) string {
	sanitized := fileNameUnsafeCharsRegex.ReplaceAllString(name, "_")

	sanitized = strings.Trim(sanitized, "_")

	if _, isReserved := reservedNames[strings.ToLower(sanitized)]; isReserved {
		sanitized = "_" + sanitized
	}
	if sanitized == "" {
		return "_"
	}

	const maxComponentLength = 100
	if len(sanitized) > maxComponentLength {
		sanitized = sanitized[:maxComponentLength]
	}
	return sanitized
}

func GenerateCacheKey(repoName, relativePath string) (string, error) {
	if !IsRepoNameSafe(repoName) {
		return "", fmt.Errorf("invalid repository name for cache key: %s", repoName)
	}

	cleanedRelativePath := filepath.ToSlash(filepath.Clean(relativePath))
	if strings.HasPrefix(cleanedRelativePath, "/") {
		cleanedRelativePath = strings.TrimPrefix(cleanedRelativePath, "/")
	}
	if cleanedRelativePath == "." {
		cleanedRelativePath = ""
	}

	parts := []string{repoName}
	if cleanedRelativePath != "" {
		pathComponents := strings.Split(cleanedRelativePath, "/")
		for _, pc := range pathComponents {
			if pc == "" || pc == "." || pc == ".." {
				continue
			}
			sanitizedComponent := SanitizeCacheKeyPathComponent(pc)
			if sanitizedComponent == "_" && pc != "_" {

			}
			parts = append(parts, sanitizedComponent)
		}
	}
	return strings.Join(parts, "/"), nil
}

func GetContentType(filePath string) string {
	ext := strings.ToLower(filepath.Ext(filePath))
	baseName := strings.ToLower(filepath.Base(filePath))

	switch baseName {
	case "release", "inrelease":
		return "text/plain; charset=utf-8"
	}
	if strings.HasPrefix(baseName, "packages") || strings.HasPrefix(baseName, "sources") ||
		strings.HasPrefix(baseName, "translation") || strings.HasPrefix(baseName, "contents") {

	}

	switch ext {
	case ".deb", ".udeb", ".ddeb":
		return "application/vnd.debian.binary-package"
	case ".dsc":
		return "text/plain; charset=utf-8"
	case ".changes":
		return "text/plain; charset=utf-8"
	case ".gz":
		return "application/gzip"
	case ".bz2":
		return "application/x-bzip2"
	case ".xz":
		return "application/x-xz"
	case ".lz4":
		return "application/x-lz4"
	case ".zst", ".zstd":
		return "application/zstd"
	case ".diff", ".patch":
		return "text/x-diff; charset=utf-8"
	case ".list":
		return "text/plain; charset=utf-8"
	}

	mimeType := mime.TypeByExtension(ext)
	if mimeType == "" {
		if strings.HasPrefix(baseName, "packages") || strings.HasPrefix(baseName, "sources") ||
			strings.HasPrefix(baseName, "translation") || strings.HasPrefix(baseName, "contents") {
			return "text/plain; charset=utf-8"
		}
		return "application/octet-stream"
	}
	return mimeType
}

func CopyHeader(h http.Header) http.Header {
	if h == nil {
		return nil
	}
	h2 := make(http.Header, len(h))
	for k, vv := range h {
		vv2 := make([]string, len(vv))
		copy(vv2, vv)
		h2[k] = vv2
	}
	return h2
}

func SelectProxyHeaders(dst, src http.Header) {

	headersToConsider := []string{
		"Cache-Control",
		"Content-Disposition",

		"Content-Language",

		"Date",
		"ETag",
		"Expires",
		"Last-Modified",
		"Link",
		"Pragma",
		"Retry-After",
		"Server",
		"Vary",
		"Age",
		"Accept-Ranges",
	}

	for _, key := range headersToConsider {
		if values := src.Values(key); len(values) > 0 {
			dst[http.CanonicalHeaderKey(key)] = values
		}
	}
}

func ParseCacheControl(headerValue string) map[string]string {
	directives := make(map[string]string)
	parts := strings.Split(headerValue, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		key := strings.ToLower(strings.TrimSpace(kv[0]))
		var value string
		if len(kv) == 2 {
			value = strings.Trim(strings.TrimSpace(kv[1]), "\"")
		}
		directives[key] = value
	}
	return directives
}

func IsClientDisconnectedError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) ||
		errors.Is(err, http.ErrAbortHandler) ||
		strings.Contains(err.Error(), "broken pipe") ||
		strings.Contains(err.Error(), "connection reset by peer") ||
		strings.Contains(err.Error(), "client disconnected") ||
		errors.Is(err, filepath.SkipDir) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || !netErr.Temporary()) {

	}
	return false
}

func FormatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.3fµs", float64(d.Nanoseconds())/1000.0)
	}
	if d < time.Second {
		return fmt.Sprintf("%.3fms", float64(d.Nanoseconds())/1000000.0)
	}
	return fmt.Sprintf("%.3fs", d.Seconds())
}

func SanitizePathForFilesystem(relativePath string) (string, error) {

	cleaned := filepath.ToSlash(filepath.Clean(relativePath))

	if filepath.IsAbs(cleaned) {
		return "", errors.New("path must be relative")
	}
	if cleaned == "." || cleaned == "" {
		return "", errors.New("path cannot be empty or dot")
	}
	if strings.HasPrefix(cleaned, "/") {
		cleaned = strings.TrimPrefix(cleaned, "/")
	}

	parts := strings.Split(cleaned, "/")
	var sanitizedParts []string

	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		if part == ".." {

			return "", fmt.Errorf("path traversal attempt detected ('..') in component: %s", part)
		}

		sanitizedComponent := SanitizeCacheKeyPathComponent(part)
		if sanitizedComponent == "" || sanitizedComponent == "_" && part != "_" {
			return "", fmt.Errorf("path component '%s' sanitized to an invalid or empty string '%s'", part, sanitizedComponent)
		}
		sanitizedParts = append(sanitizedParts, sanitizedComponent)
	}

	if len(sanitizedParts) == 0 {
		return "", errors.New("sanitized path resulted in no valid components")
	}

	return filepath.Join(sanitizedParts...), nil
}

func CompareETags(clientETagHeader string, resourceETag string) bool {
	if clientETagHeader == "" || resourceETag == "" {
		return false
	}

	if clientETagHeader == "*" {
		return true
	}

	normResETag := strings.TrimPrefix(resourceETag, "W/")
	isResWeak := strings.HasPrefix(resourceETag, "W/")

	tags := strings.Split(clientETagHeader, ",")
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}

		isClientTagWeak := strings.HasPrefix(tag, "W/")
		normClientTag := strings.TrimPrefix(tag, "W/")

		if normClientTag == normResETag {

			_ = isResWeak
			_ = isClientTagWeak
			return true
		}
	}
	return false
}
