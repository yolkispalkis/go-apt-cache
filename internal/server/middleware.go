// internal/server/middleware.go
package server

import (
	"net/http"
	"runtime/debug"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/logging"
)

type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
}

func (w *responseWriterInterceptor) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *responseWriterInterceptor) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytesWritten += n
	return n, err
}

// LoggingMiddleware logs request details.
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		interceptor := &responseWriterInterceptor{ResponseWriter: w, statusCode: http.StatusOK} // Default OK

		next.ServeHTTP(interceptor, r)

		duration := time.Since(start)
		logging.Info("Request: [%s] \"%s %s %s\" %d %d %s \"%s\" (%s)",
			r.RemoteAddr,
			r.Method,
			r.URL.RequestURI(),
			r.Proto,
			interceptor.statusCode,
			interceptor.bytesWritten,
			r.Header.Get("User-Agent"),
			r.Header.Get("Referer"), // If applicable
			duration,
		)
	})
}

// RecoveryMiddleware recovers from panics and logs them.
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				logging.Error("Panic recovered: %v\n%s", err, debug.Stack())
				// Prevent further writes if headers haven't been sent
				if rw, ok := w.(interface{ Status() int }); !ok || rw.Status() == 0 {
					http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				}
			}
		}()
		next.ServeHTTP(w, r)
	})
}
