package server

import (
	"net/http"
	"runtime/debug"
	"time"

	"github.com/rs/zerolog/log"
)

type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
}

func (w *responseWriterInterceptor) WriteHeader(statusCode int) {
	if w.statusCode == 0 {
		w.statusCode = statusCode
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *responseWriterInterceptor) Write(b []byte) (int, error) {
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytesWritten += n
	return n, err
}

func (w *responseWriterInterceptor) Status() int {
	if w.statusCode == 0 {
		return http.StatusOK
	}
	return w.statusCode
}

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		interceptor := &responseWriterInterceptor{ResponseWriter: w, statusCode: 0}

		next.ServeHTTP(interceptor, r)

		duration := time.Since(start)
		logEvent := log.Info()

		logEvent.Str("remote_addr", r.RemoteAddr).
			Str("method", r.Method).
			Str("uri", r.URL.RequestURI()).
			Str("proto", r.Proto).
			Int("status_code", interceptor.Status()).
			Int("bytes_written", interceptor.bytesWritten).
			Dur("duration_ms", duration)

		if userAgent := r.Header.Get("User-Agent"); userAgent != "" {
			logEvent.Str("user_agent", userAgent)
		}
		if referer := r.Header.Get("Referer"); referer != "" {
			logEvent.Str("referer", referer)
		}

		logEvent.Msg("Request handled")
	})
}

func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Error().
					Interface("panic_error", err).
					Str("stack", string(debug.Stack())).
					Msg("Panic recovered")

				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
