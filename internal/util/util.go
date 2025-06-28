package util

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
)

var (
	sizeRe     = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGT])?B?$`)
	repoNameRe = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)
)

var headerProxyWhitelist = map[string]struct{}{
	"Accept-Ranges": {}, "Cache-Control": {}, "Content-Length": {},
	"Content-Type": {}, "Date": {}, "ETag": {}, "Last-Modified": {},
	"Content-Disposition": {}, "Expires": {}, "Vary": {}, "Age": {},
}

var headerPool = sync.Pool{New: func() any { return make(http.Header, 16) }}
var bufferPoolSize int64 = 64 * 1024
var bufferPool = sync.Pool{New: func() any { return make([]byte, bufferPoolSize) }}

func InitBufferPool(sizeStr string, log *logging.Logger) {
	size, err := ParseSize(sizeStr)
	if err != nil || size <= 0 {
		log.Warn("Invalid bufferSize, using default.", "configured_size", sizeStr, "default_size", bufferPoolSize, "error", err)
		return
	}
	bufferPoolSize = size
	bufferPool = sync.Pool{New: func() any { return make([]byte, bufferPoolSize) }}
	log.Info("Buffer pool initialized with configured size", "size", FormatSize(bufferPoolSize))
}

func MustParseSize(s string) int64 {
	size, err := ParseSize(s)
	if err != nil {
		panic(err)
	}
	return size
}

func ParseSize(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("size string is empty")
	}
	trimmed := strings.ToUpper(strings.TrimSpace(s))
	m := sizeRe.FindStringSubmatch(trimmed)
	if m == nil {
		if b, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return b, nil
		}
		return 0, fmt.Errorf("invalid size format: %q", s)
	}
	val, _ := strconv.ParseFloat(m[1], 64)
	var mult float64 = 1
	switch m[2] {
	case "K":
		mult = 1 << 10
	case "M":
		mult = 1 << 20
	case "G":
		mult = 1 << 30
	case "T":
		mult = 1 << 40
	}
	return int64(val * mult), nil
}

func FormatSize(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGT"[exp])
}

func CleanPath(p string) string { return filepath.Clean(p) }

func IsRepoNameSafe(n string) bool {
	if n == "" || n == "." || n == ".." || strings.ContainsAny(n, "/\\") {
		return false
	}
	return repoNameRe.MatchString(n)
}

func CopyHeader(h http.Header) http.Header {
	if h == nil {
		return nil
	}
	h2 := headerPool.Get().(http.Header)
	for k := range h2 {
		delete(h2, k)
	}
	for k, vv := range h {
		h2[k] = append(h2[k][:0], vv...)
	}
	return h2
}

func ReturnHeader(h http.Header) {
	if h != nil {
		headerPool.Put(h)
	}
}

func GetBuffer() []byte { return bufferPool.Get().([]byte) }

func ReturnBuffer(buf []byte) {
	if int64(cap(buf)) == bufferPoolSize {
		bufferPool.Put(buf[:cap(buf)])
	}
}

func CopyWhitelistedHeaders(dst, src http.Header) {
	for key, values := range src {
		if _, ok := headerProxyWhitelist[http.CanonicalHeaderKey(key)]; ok {
			dst[key] = values
		}
	}
}

func UpdateCacheHeaders(dst, src http.Header) {
	for _, key := range []string{"Date", "Expires", "Cache-Control", "ETag", "Last-Modified", "Age"} {
		if val := src.Get(key); val != "" {
			dst.Set(key, val)
		}
	}
}

func ParseCacheControl(v string) map[string]string {
	dirs := make(map[string]string)
	for _, p := range strings.Split(v, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if key, val, ok := strings.Cut(p, "="); ok {
			dirs[strings.ToLower(key)] = strings.Trim(val, `"`)
		} else {
			dirs[strings.ToLower(p)] = ""
		}
	}
	return dirs
}

func IsClientDisconnectedError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, http.ErrAbortHandler) || errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	errStr := err.Error()
	return strings.Contains(errStr, "broken pipe") || strings.Contains(errStr, "connection reset by peer")
}

func CompareETags(clientETagsStr, resourceETag string) bool {
	if clientETagsStr == "" || resourceETag == "" {
		return false
	}
	if clientETagsStr == "*" {
		return true
	}
	normResourceETag := strings.Trim(strings.TrimPrefix(resourceETag, "W/"), `"`)
	for _, cTag := range strings.Split(clientETagsStr, ",") {
		normClientTag := strings.Trim(strings.TrimPrefix(strings.TrimSpace(cTag), "W/"), `"`)
		if normClientTag == normResourceETag {
			return true
		}
	}
	return false
}

func CalculateFreshness(headers http.Header, responseTime time.Time, overrides []config.CacheOverride) time.Time {
	// 1. Проверяем правила переопределения из конфига.
	// TODO: Передавать сюда relPath для проверки. Для простоты пока опускаем.
	// if overrideTTL, ok := findOverrideTTL(relPath, overrides); ok {
	// 	return responseTime.Add(overrideTTL)
	// }

	// 2. Используем стандартную логику кеширования HTTP (RFC 9111).
	cc := ParseCacheControl(headers.Get("Cache-Control"))

	if _, ok := cc["no-store"]; ok {
		return time.Time{} // Не кешировать
	}
	if _, ok := cc["no-cache"]; ok {
		return responseTime // Требует ревалидации
	}

	var lifetime time.Duration
	if sMaxAge, ok := cc["s-maxage"]; ok {
		if sec, err := strconv.ParseInt(sMaxAge, 10, 64); err == nil {
			lifetime = time.Duration(sec) * time.Second
		}
	} else if maxAge, ok := cc["max-age"]; ok {
		if sec, err := strconv.ParseInt(maxAge, 10, 64); err == nil {
			lifetime = time.Duration(sec) * time.Second
		}
	} else if expiresStr := headers.Get("Expires"); expiresStr != "" {
		if expires, err := http.ParseTime(expiresStr); err == nil {
			lifetime = expires.Sub(responseTime)
		}
	} else if lmStr := headers.Get("Last-Modified"); lmStr != "" {
		if lm, err := http.ParseTime(lmStr); err == nil {
			lifetime = responseTime.Sub(lm) / 10 // Эвристика: 10%
		}
	}

	if lifetime < 0 {
		lifetime = 0
	}

	return responseTime.Add(lifetime)
}

func findOverrideTTL(relPath string, overrides []config.CacheOverride) (time.Duration, bool) {
	for _, rule := range overrides {
		matched, err := doublestar.Match(rule.PathPattern, relPath)
		if err == nil && matched {
			return rule.TTL, true
		}
	}
	return 0, false
}

// ResponseWriterInterceptor для перехвата статуса и размера ответа.
type ResponseWriterInterceptor struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func NewResponseWriterInterceptor(w http.ResponseWriter) *ResponseWriterInterceptor {
	return &ResponseWriterInterceptor{ResponseWriter: w}
}

func (w *ResponseWriterInterceptor) WriteHeader(statusCode int) {
	if w.wroteHeader {
		return
	}
	w.status = statusCode
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *ResponseWriterInterceptor) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

func (w *ResponseWriterInterceptor) Status() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (w *ResponseWriterInterceptor) BytesWritten() int64 {
	return w.bytes
}

// CheckConditional проверяет заголовки If-None-Match и If-Modified-Since.
func CheckConditional(w http.ResponseWriter, r *http.Request, headers http.Header) bool {
	etag := headers.Get("ETag")
	lastModified := headers.Get("Last-Modified")

	if etag != "" {
		if inm := r.Header.Get("If-None-Match"); inm != "" && CompareETags(inm, etag) {
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	}

	if lastModified != "" {
		if ims := r.Header.Get("If-Modified-Since"); ims != "" {
			if t, err := http.ParseTime(lastModified); err == nil {
				if t2, err2 := http.ParseTime(ims); err2 == nil {
					if !t.After(t2) {
						w.WriteHeader(http.StatusNotModified)
						return true
					}
				}
			}
		}
	}
	return false
}
