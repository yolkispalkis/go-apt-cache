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
)

const (
	reSize = `^(\d+(?:\.\d+)?)\s*([KMGT])?B?$`
	reRepo = `^[a-zA-Z0-9._-]+$`
)

var (
	sizeRe     = regexp.MustCompile(reSize)
	repoNameRe = regexp.MustCompile(reRepo)
)

func ParseSize(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("empty size")
	}
	trim := strings.ToUpper(strings.TrimSpace(s))
	if m := sizeRe.FindStringSubmatch(trim); m != nil {
		val, _ := strconv.ParseFloat(m[1], 64)
		val = math.Round(val)
		mul := float64(1)
		switch m[2] {
		case "K":
			mul = 1 << 10
		case "M":
			mul = 1 << 20
		case "G":
			mul = 1 << 30
		case "T":
			mul = 1 << 40
		}
		return int64(val * mul), nil
	}
	if b, err := strconv.ParseInt(trim, 10, 64); err == nil {
		return b, nil
	}
	return 0, fmt.Errorf("invalid size: %q", s)
}

func FormatSize(b int64) string {
	const u = 1024
	if b < u {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(u), 0
	for n := b / u; n >= u; n /= u {
		div *= u
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGT"[exp])
}

func IsRepoNameSafe(n string) bool {
	return n != "" && n != "." && n != ".." && !strings.ContainsAny(n, "/\\") && repoNameRe.MatchString(n)
}

// headers

var headerProxyWhitelist = map[string]struct{}{
	"Accept-Ranges":       {},
	"Cache-Control":       {},
	"Content-Length":      {},
	"Content-Type":        {},
	"Content-Range":       {}, // allow forwarding for 206
	"Date":                {},
	"ETag":                {},
	"Last-Modified":       {},
	"Content-Disposition": {},
	"Expires":             {},
	"Vary":                {},
	"Age":                 {},
}

func CopyHeader(h http.Header) http.Header {
	if h == nil {
		return http.Header{}
	}
	return h.Clone()
}

func CopyWhitelisted(dst, src http.Header) {
	for k, v := range src {
		if _, ok := headerProxyWhitelist[http.CanonicalHeaderKey(k)]; ok {
			dst[k] = v
		}
	}
}

func ParseCacheControl(v string) map[string]string {
	out := map[string]string{}
	for _, f := range strings.Split(v, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		k, val, ok := strings.Cut(f, "=")
		k = strings.ToLower(k)
		if ok {
			val = strings.Trim(val, `"`)
		}
		out[k] = val
	}
	return out
}

func ParseUint(val string) (uint64, bool) {
	if val == "" {
		return 0, false
	}
	n, err := strconv.ParseUint(val, 10, 64)
	return n, err == nil
}

// ETag weak match (If-None-Match) per RFC
func WeakMatchETag(list, etag string) bool {
	if list == "" || etag == "" {
		return false
	}
	if list == "*" {
		return true
	}
	norm := func(s string) string {
		s = strings.TrimSpace(s)
		return strings.Trim(s, `"`)
	}
	target := strings.TrimPrefix(norm(etag), "W/")
	for _, part := range strings.Split(list, ",") {
		if strings.TrimPrefix(norm(part), "W/") == target {
			return true
		}
	}
	return false
}

// Backward-compat: old helper that immediately writes 304
func CheckConditionalHIT(w http.ResponseWriter, r *http.Request, hdr http.Header) bool {
	if WeakMatchETag(r.Header.Get("If-None-Match"), hdr.Get("ETag")) {
		w.WriteHeader(http.StatusNotModified)
		return true
	}
	if lm := hdr.Get("Last-Modified"); lm != "" && r.Header.Get("If-Modified-Since") != "" {
		if lmt, err := http.ParseTime(lm); err == nil {
			if ims, err2 := http.ParseTime(r.Header.Get("If-Modified-Since")); err2 == nil && !lmt.After(ims) {
				w.WriteHeader(http.StatusNotModified)
				return true
			}
		}
	}
	return false
}

// New: only check the condition without writing the response
func IsNotModified(r *http.Request, hdr http.Header) bool {
	if WeakMatchETag(r.Header.Get("If-None-Match"), hdr.Get("ETag")) {
		return true
	}
	if lm := hdr.Get("Last-Modified"); lm != "" && r.Header.Get("If-Modified-Since") != "" {
		if lmt, err := http.ParseTime(lm); err == nil {
			if ims, err2 := http.ParseTime(r.Header.Get("If-Modified-Since")); err2 == nil && !lmt.After(ims) {
				return true
			}
		}
	}
	return false
}

func ClientHasValidators(r *http.Request) bool {
	return r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != ""
}

// buffers

var (
	bufSize int64 = 64 * 1024
	pool          = sync.Pool{New: func() any { return make([]byte, bufSize) }}
)

type loggerIface interface {
	Info() *logEventStub
	Warn() *logEventStub
}

type logEventStub struct{}

func (*logEventStub) Str(string, string) *logEventStub  { return &logEventStub{} }
func (*logEventStub) Int64(string, int64) *logEventStub { return &logEventStub{} }
func (*logEventStub) Msg(string)                        {}

func InitBufferPool(sizeStr string, _ any) {
	if s, err := ParseSize(sizeStr); err == nil && s > 0 {
		bufSize = s
		pool = sync.Pool{New: func() any { return make([]byte, bufSize) }}
	}
}

func GetBuffer() []byte { return pool.Get().([]byte) }
func PutBuffer(b []byte) {
	if int64(cap(b)) == bufSize {
		pool.Put(b[:cap(b)])
	}
}
func CleanPath(p string) string { return filepath.Clean(p) }

// network errs

func IsClientDisconnect(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, http.ErrAbortHandler) || errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "broken pipe") || strings.Contains(s, "connection reset by peer")
}

// logging response writer

type RespLogWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
	wrote  bool
}

func NewRespLogWriter(w http.ResponseWriter) *RespLogWriter { return &RespLogWriter{ResponseWriter: w} }

func (w *RespLogWriter) WriteHeader(code int) {
	if w.wrote {
		return
	}
	w.status, w.wrote = code, true
	w.ResponseWriter.WriteHeader(code)
}
func (w *RespLogWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}
func (w *RespLogWriter) Status() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}
func (w *RespLogWriter) Bytes() int64 { return w.bytes }
