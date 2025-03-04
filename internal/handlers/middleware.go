package handlers

import (
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
)

// Middleware represents a middleware function
type Middleware func(http.Handler) http.Handler

// MiddlewareChain represents a chain of middleware
type MiddlewareChain []Middleware

// Apply applies all middleware in the chain to the handler
func (mc MiddlewareChain) Apply(handler http.Handler) http.Handler {
	for i := len(mc) - 1; i >= 0; i-- {
		handler = mc[i](handler)
	}
	return handler
}

// Chain creates a new middleware chain
func Chain(middlewares ...Middleware) MiddlewareChain {
	return MiddlewareChain(middlewares)
}

// LoggingMiddleware logs HTTP requests
type LoggingMiddleware struct {
	next http.Handler
}

// NewLoggingMiddleware creates a new logging middleware
func NewLoggingMiddleware(next http.Handler) http.Handler {
	return &LoggingMiddleware{next: next}
}

// ServeHTTP implements the http.Handler interface
func (lm *LoggingMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// Create a response writer wrapper to capture status code
	lrw := &loggingResponseWriter{
		ResponseWriter: w,
		statusCode:     http.StatusOK, // Default to 200 OK
	}

	// Call the next handler
	lm.next.ServeHTTP(lrw, r)

	// Log the request
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

// loggingResponseWriter is a wrapper for http.ResponseWriter that captures the status code
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
}

// WriteHeader captures the status code
func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

// Write captures the number of bytes written
func (lrw *loggingResponseWriter) Write(b []byte) (int, error) {
	n, err := lrw.ResponseWriter.Write(b)
	lrw.bytesWritten += int64(n)
	return n, err
}

// ReverseProxyMiddleware implements middleware for reverse proxy functionality
type ReverseProxyMiddleware struct {
	next   http.Handler
	config *config.Config
}

// NewReverseProxyMiddleware creates a new reverse proxy middleware
func NewReverseProxyMiddleware(next http.Handler, cfg *config.Config) http.Handler {
	return &ReverseProxyMiddleware{
		next:   next,
		config: cfg,
	}
}

// ServeHTTP implements the http.Handler interface
func (m *ReverseProxyMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Add X-Forwarded-For header
	if clientIP, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		if prior, ok := r.Header["X-Forwarded-For"]; ok {
			clientIP = strings.Join(prior, ", ") + ", " + clientIP
		}
		r.Header.Set("X-Forwarded-For", clientIP)
	}

	// Add X-Forwarded-Proto header
	if r.TLS != nil {
		r.Header.Set("X-Forwarded-Proto", "https")
	} else {
		r.Header.Set("X-Forwarded-Proto", "http")
	}

	m.next.ServeHTTP(w, r)
}

// GetConfig returns the configuration
func (m *ReverseProxyMiddleware) GetConfig() *config.Config {
	return m.config
}

// CreateMiddlewareChain creates a middleware chain based on configuration
func CreateMiddlewareChain(cfg *config.Config) MiddlewareChain {
	var middlewares []Middleware

	// Add reverse proxy middleware
	middlewares = append(middlewares, func(next http.Handler) http.Handler {
		return NewReverseProxyMiddleware(next, cfg)
	})

	// Add logging middleware if enabled
	if cfg.Server.LogRequests {
		middlewares = append(middlewares, NewLoggingMiddleware)
	}

	return Chain(middlewares...)
}
