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
	sizeRe     = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGT])?B?$`)
	repoNameRe = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

	fsUnsafeRe = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F\s]+`)

	reservedFsNames = map[string]struct{}{
		"con": {}, "prn": {}, "aux": {}, "nul": {},
		"com1": {}, "com2": {}, "com3": {}, "com4": {}, "com5": {}, "com6": {}, "com7": {}, "com8": {}, "com9": {},
		"lpt1": {}, "lpt2": {}, "lpt3": {}, "lpt4": {}, "lpt5": {}, "lpt6": {}, "lpt7": {}, "lpt8": {}, "lpt9": {},
	}
)

func ParseSize(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("size string empty")
	}
	m := sizeRe.FindStringSubmatch(strings.ToUpper(strings.TrimSpace(s)))
	if m == nil {

		b, err := strconv.ParseInt(s, 10, 64)
		if err == nil && b >= 0 {
			return b, nil
		}
		return 0, fmt.Errorf("invalid size format: %q (expected e.g., '10GB', '500MB', '1024')", s)
	}
	val, err := strconv.ParseFloat(m[1], 64)
	if err != nil || val < 0 {
		return 0, fmt.Errorf("invalid numeric value in size %q", s)
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
	}
	byteSize := int64(val * mult)

	if val > 0 && byteSize <= 0 && mult > 1.0 {
		return 0, fmt.Errorf("size value %q resulted in non-positive bytes or overflow", s)
	}
	return byteSize, nil
}

func FormatSize(b int64) string {
	if b < 0 {
		return fmt.Sprintf("%d B (Negative)", b)
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

func SanitizeFSComponent(name string) string {
	s := fsUnsafeRe.ReplaceAllString(name, "_")
	s = strings.Trim(s, "_")
	if _, reserved := reservedFsNames[strings.ToLower(s)]; reserved {
		s = "_" + s
	}
	if s == "" {
		return "_"
	}
	const maxLen = 100
	if len(s) > maxLen {
		s = s[:maxLen]
	}
	return s
}

func GenerateCacheKey(repo, relPath string) (string, error) {
	if !IsRepoNameSafe(repo) {
		return "", fmt.Errorf("invalid repo name for cache key: %s", repo)
	}

	cleanRelPath := filepath.ToSlash(filepath.Clean(relPath))
	if strings.HasPrefix(cleanRelPath, "/") {
		cleanRelPath = strings.TrimPrefix(cleanRelPath, "/")
	}
	if cleanRelPath == "." || cleanRelPath == "" {
		return SanitizeFSComponent(repo), nil
	}

	var sb strings.Builder
	sb.WriteString(SanitizeFSComponent(repo))

	for _, p := range strings.Split(cleanRelPath, "/") {
		if p == "" || p == "." || p == ".." {
			continue
		}
		sb.WriteByte('/')
		sb.WriteString(SanitizeFSComponent(p))
	}
	return sb.String(), nil
}

func GetContentType(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	base := strings.ToLower(filepath.Base(path))

	switch base {
	case "release", "inrelease":
		return "text/plain; charset=utf-8"
	}

	if strings.HasPrefix(base, "packages") ||
		strings.HasPrefix(base, "sources") ||
		strings.HasPrefix(base, "translation") ||
		strings.HasPrefix(base, "contents") {

	}

	switch ext {
	case ".deb", ".udeb", ".ddeb":
		return "application/vnd.debian.binary-package"
	case ".dsc", ".changes", ".list":
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
	}

	ct := mime.TypeByExtension(ext)
	if ct == "" {

		if strings.HasPrefix(base, "packages") ||
			strings.HasPrefix(base, "sources") ||
			strings.HasPrefix(base, "translation") ||
			strings.HasPrefix(base, "contents") {
			return "text/plain; charset=utf-8"
		}
		return "application/octet-stream"
	}
	return ct
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

	toConsider := []string{
		"Cache-Control", "Content-Disposition", "Content-Language", "Date", "ETag",
		"Expires", "Last-Modified", "Link", "Pragma", "Retry-After", "Server",
		"Vary", "Age", "Accept-Ranges",
	}
	for _, key := range toConsider {
		if vals := src.Values(key); len(vals) > 0 {

			dst[http.CanonicalHeaderKey(key)] = vals
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

	if errors.Is(err, context.Canceled) ||
		errors.Is(err, http.ErrAbortHandler) ||
		strings.Contains(err.Error(), "broken pipe") ||
		strings.Contains(err.Error(), "connection reset by peer") {
		return true
	}

	var netErr net.Error

	return errors.As(err, &netErr) && (netErr.Timeout() || !netErr.Temporary())
}

func FormatDuration(d time.Duration) string {
	switch {
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
		cTag = strings.TrimSpace(cTag)
		if cTag == "" {
			continue
		}

		normClientTag := strings.TrimPrefix(cTag, "W/")
		normClientTag = strings.Trim(normClientTag, "\"")

		if normClientTag == normResourceETag {
			return true
		}
	}
	return false
}
