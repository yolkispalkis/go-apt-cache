package handlers

import (
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
)

type Middleware func(http.Handler) http.Handler

type MiddlewareChain []Middleware

func (mc MiddlewareChain) Apply(handler http.Handler) http.Handler {
	for i := len(mc) - 1; i >= 0; i-- {
		handler = mc[i](handler)
	}
	return handler
}

func Chain(middlewares ...Middleware) MiddlewareChain {
	return MiddlewareChain(middlewares)
}

type LoggingMiddleware struct {
	next http.Handler
}

func NewLoggingMiddleware(next http.Handler) http.Handler {
	return &LoggingMiddleware{next: next}
}

func (lm *LoggingMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	lrw := &loggingResponseWriter{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
	}

	lm.next.ServeHTTP(lrw, r)

	duration := time.Since(start)
	logging.Info("%s %s %s %d %d %s",
		r.RemoteAddr,
		r.Method,
		r.URL.Path,
		lrw.statusCode,
		lrw.bytesWritten,
		duration,
	)
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func (lrw *loggingResponseWriter) Write(b []byte) (int, error) {
	n, err := lrw.ResponseWriter.Write(b)
	lrw.bytesWritten += int64(n)
	return n, err
}

type ReverseProxyMiddleware struct {
	next   http.Handler
	config *config.Config
}

func NewReverseProxyMiddleware(next http.Handler, cfg *config.Config) http.Handler {
	return &ReverseProxyMiddleware{
		next:   next,
		config: cfg,
	}
}

func (m *ReverseProxyMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if clientIP, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		if prior, ok := r.Header["X-Forwarded-For"]; ok {
			clientIP = strings.Join(prior, ", ") + ", " + clientIP
		}
		r.Header.Set("X-Forwarded-For", clientIP)
	}

	if r.TLS != nil {
		r.Header.Set("X-Forwarded-Proto", "https")
	} else {
		r.Header.Set("X-Forwarded-Proto", "http")
	}

	m.next.ServeHTTP(w, r)
}

func (m *ReverseProxyMiddleware) GetConfig() *config.Config {
	return m.config
}

func CreateMiddlewareChain(cfg *config.Config) MiddlewareChain {
	var middlewares []Middleware

	middlewares = append(middlewares, func(next http.Handler) http.Handler {
		return NewReverseProxyMiddleware(next, cfg)
	})

	if cfg.Server.LogRequests {
		middlewares = append(middlewares, NewLoggingMiddleware)
	}

	return Chain(middlewares...)
}
