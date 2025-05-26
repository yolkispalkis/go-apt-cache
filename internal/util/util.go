package util

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	sizeRe     = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGT])?B?$`)
	repoNameRe = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)
)

var headerProxyWhitelist = map[string]struct{}{
	"Accept-Ranges":       {},
	"Cache-Control":       {},
	"Content-Length":      {},
	"Content-Type":        {},
	"Date":                {},
	"ETag":                {},
	"Last-Modified":       {},
	"Content-Disposition": {},
	"Expires":             {},
	"Vary":                {},
	"Age":                 {},
}

func RepoNameRegexString() string {
	return repoNameRe.String()
}

func ParseSize(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("size string empty")
	}
	trimmedSizeStr := strings.ToUpper(strings.TrimSpace(s))
	m := sizeRe.FindStringSubmatch(trimmedSizeStr)

	if m == nil {
		b, err := strconv.ParseInt(trimmedSizeStr, 10, 64)
		if err == nil && b >= 0 {
			return b, nil
		}
		return 0, fmt.Errorf("invalid size format: %q (expected e.g., '10GB', '500MB', '1024')", s)
	}

	val, err := strconv.ParseFloat(m[1], 64)
	if err != nil || val < 0 {
		return 0, fmt.Errorf("invalid numeric value in size %q: %w", s, err)
	}

	var mult float64 = 1.0
	switch m[2] {
	case "K":
		mult = 1024
	case "M":
		mult = 1024 * 1024
	case "G":
		mult = 1024 * 1024 * 1024
	case "T":
		mult = 1024 * 1024 * 1024 * 1024
	case "":
		mult = 1.0
	}
	byteSize := int64(val * mult)

	if val > 0 && byteSize <= 0 && mult > 1.0 {
		return 0, fmt.Errorf("size value %q (%f * %f) resulted in non-positive bytes or overflow (%d)", s, val, mult, byteSize)
	}
	return byteSize, nil
}

func FormatSize(b int64) string {
	if b < 0 {
		return fmt.Sprintf("%dB (Negative)", b)
	}
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit && exp < 3; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGT"[exp])
}

func CleanPath(p string) string {
	if p == "" {
		return "."
	}
	return filepath.Clean(p)
}

func IsRepoNameSafe(n string) bool {
	if n == "" || n == "." || n == ".." || strings.ContainsAny(n, "/\\") {
		return false
	}
	return repoNameRe.MatchString(n)
}

// GenerateCacheKey теперь просто объединяет repo и path
func GenerateCacheKey(repo, relPath string) (string, error) {
	if !IsRepoNameSafe(repo) {
		return "", fmt.Errorf("invalid repo name for cache key: %q", repo)
	}

	// Просто объединяем repo и путь - хеширование будет в disk_store
	if relPath == "" {
		return repo, nil
	}

	// Нормализуем путь
	cleanPath := strings.TrimPrefix(relPath, "/")
	if cleanPath == "" || cleanPath == "." {
		return repo, nil
	}

	return repo + "/" + cleanPath, nil
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

func CopyWhitelistedHeaders(dst, src http.Header) {
	if src == nil {
		return
	}
	for keySrc, valuesSrc := range src {
		canonicalKey := http.CanonicalHeaderKey(keySrc)
		if _, ok := headerProxyWhitelist[canonicalKey]; ok {
			dst[canonicalKey] = append([]string(nil), valuesSrc...)
		}
	}
}

func ParseCacheControl(v string) map[string]string {
	dirs := make(map[string]string)
	parts := strings.Split(v, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		key := strings.ToLower(strings.TrimSpace(kv[0]))
		val := ""
		if len(kv) == 2 {
			val = strings.Trim(strings.TrimSpace(kv[1]), "\"")
		}
		dirs[key] = val
	}
	return dirs
}

func IsClientDisconnectedError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, http.ErrAbortHandler) {
		return true
	}
	if errors.Is(err, io.ErrClosedPipe) {
		return true
	}

	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "client disconnected") ||
		strings.Contains(errStr, "protocol wrong type for socket") {
		return true
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if IsClientDisconnectedError(opErr.Err) {
			return true
		}
	}
	return false
}

func FormatDuration(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.3fµs", float64(d.Nanoseconds())/1e3)
	case d < time.Second:
		return fmt.Sprintf("%.3fms", float64(d.Nanoseconds())/1e6)
	default:
		return fmt.Sprintf("%.3fs", d.Seconds())
	}
}

func CompareETags(clientETagsStr string, resourceETag string) bool {
	if clientETagsStr == "" || resourceETag == "" {
		return false
	}

	if clientETagsStr == "*" {
		return true
	}

	normResourceETag := strings.TrimPrefix(resourceETag, "W/")
	normResourceETag = strings.Trim(normResourceETag, "\"")

	clientTags := strings.Split(clientETagsStr, ",")
	for _, cTag := range clientTags {
		cTagTrimmed := strings.TrimSpace(cTag)
		if cTagTrimmed == "" {
			continue
		}

		normClientTag := strings.TrimPrefix(cTagTrimmed, "W/")
		normClientTag = strings.Trim(normClientTag, "\"")

		if normClientTag == normResourceETag {
			return true
		}
	}
	return false
}

func ResolveURL(base, ref string) (string, error) {
	if ref == "" {
		return base, nil
	}

	refURL, err := url.Parse(ref)
	if err != nil {
		return "", fmt.Errorf("parsing reference URL %q: %w", ref, err)
	}

	if refURL.IsAbs() {
		return ref, nil
	}

	baseURL, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("parsing base URL %q: %w", base, err)
	}

	return baseURL.ResolveReference(refURL).String(), nil
}
