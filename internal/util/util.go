package util

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/yolkispalkis/go-apt-cache/internal/logging"
)

const (
	sizePattern = `^(\d+(?:\.\d+)?)\s*([KMGT])?B?$`
	repoPattern = `^[a-zA-Z0-9._-]+$`

	kb = 1 << 10
	mb = 1 << 20
	gb = 1 << 30
	tb = 1 << 40
)

var (
	sizeRe     = regexp.MustCompile(sizePattern)
	repoNameRe = regexp.MustCompile(repoPattern)
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

var (
	bufferPoolSize int64 = 64 * 1024
	bufferPool           = sync.Pool{New: func() any { return make([]byte, bufferPoolSize) }}
)

func InitBufferPool(sizeStr string, log *logging.Logger) {
	size, err := ParseSize(sizeStr)
	if err != nil || size <= 0 {
		log.Warn().
			Str("configured_size", sizeStr).
			Int64("default_size", bufferPoolSize).
			Err(err).
			Msg("Invalid bufferSize, using default.")
		return
	}
	bufferPoolSize = size
	bufferPool = sync.Pool{New: func() any { return make([]byte, bufferPoolSize) }}
	log.Info().Str("size", FormatSize(bufferPoolSize)).Msg("Buffer pool initialized with configured size")
}

func GetBuffer() []byte { return bufferPool.Get().([]byte) }
func ReturnBuffer(buf []byte) {
	if int64(cap(buf)) == bufferPoolSize {
		bufferPool.Put(buf[:cap(buf)])
	}
}

func ParseSize(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("size string is empty")
	}
	trimmed := strings.ToUpper(strings.TrimSpace(s))
	if m := sizeRe.FindStringSubmatch(trimmed); m != nil {
		val, _ := strconv.ParseFloat(m[1], 64)
		val = math.Round(val)

		mult := float64(1)
		switch m[2] {
		case "K":
			mult = kb
		case "M":
			mult = mb
		case "G":
			mult = gb
		case "T":
			mult = tb
		}
		return int64(val * mult), nil
	}
	/* число без суффикса – трактуем как байты */
	if b, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return b, nil
	}
	return 0, fmt.Errorf("invalid size format: %q", s)
}

func MustParseSize(s string) int64 {
	v, err := ParseSize(s)
	if err != nil {
		panic(err)
	}
	return v
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
	return h.Clone()
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
		if v := src.Get(key); v != "" {
			dst.Set(key, v)
		}
	}
}

func ParseCacheControl(v string) map[string]string {
	res := make(map[string]string)
	for _, field := range strings.FieldsFunc(v, func(r rune) bool { return r == ',' }) {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		k, val, hasVal := strings.Cut(field, "=")
		k = strings.ToLower(k)
		if hasVal {
			val = strings.Trim(val, `"`)
		}
		res[k] = val
	}
	return res
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
	es := err.Error()
	return strings.Contains(es, "broken pipe") || strings.Contains(es, "connection reset by peer")
}

func normalizeETag(etag string) string { return strings.TrimPrefix(strings.Trim(etag, `"`), "W/") }

func CompareETags(clientETagsStr, resourceETag string) bool {
	if clientETagsStr == "" || resourceETag == "" {
		return false
	}
	if clientETagsStr == "*" {
		return true
	}
	resource := normalizeETag(resourceETag)
	for _, part := range strings.Split(clientETagsStr, ",") {
		if normalizeETag(strings.TrimSpace(part)) == resource {
			return true
		}
	}
	return false
}

type ResponseWriterInterceptor struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

var _ io.ReaderFrom = (*ResponseWriterInterceptor)(nil)

func NewResponseWriterInterceptor(w http.ResponseWriter) *ResponseWriterInterceptor {
	return &ResponseWriterInterceptor{ResponseWriter: w}
}

func (w *ResponseWriterInterceptor) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.status, w.wroteHeader = code, true
	w.ResponseWriter.WriteHeader(code)
}

func (w *ResponseWriterInterceptor) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

func (w *ResponseWriterInterceptor) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		if !w.wroteHeader {
			w.WriteHeader(http.StatusOK)
		}
		n, err := rf.ReadFrom(r)
		w.bytes += n
		return n, err
	}
	return io.Copy(w, r)
}

func (w *ResponseWriterInterceptor) Status() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}
func (w *ResponseWriterInterceptor) BytesWritten() int64 { return w.bytes }

func CheckConditional(w http.ResponseWriter, r *http.Request, hdr http.Header) bool {
	etag := hdr.Get("ETag")
	lastMod := hdr.Get("Last-Modified")

	if etag != "" && CompareETags(r.Header.Get("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		return true
	}
	if lastMod != "" && r.Header.Get("If-Modified-Since") != "" {
		if lmTime, err := http.ParseTime(lastMod); err == nil {
			if imsTime, err2 := http.ParseTime(r.Header.Get("If-Modified-Since")); err2 == nil && !lmTime.After(imsTime) {
				w.WriteHeader(http.StatusNotModified)
				return true
			}
		}
	}
	return false
}

func ClientHasFreshVersion(r *http.Request) bool {
	return r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != ""
}
