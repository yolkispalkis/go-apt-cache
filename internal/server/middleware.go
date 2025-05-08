package server

import (
	"context"
	"errors"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/rs/zerolog"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
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

func (w *responseWriterInterceptor) getStatusCode() int {
	if w.statusCode == 0 {
		return http.StatusOK
	}
	return w.statusCode
}

func LoggingMiddleware(logger zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			interceptor := &responseWriterInterceptor{ResponseWriter: w}

			next.ServeHTTP(interceptor, r)

			duration := time.Since(start)
			statusCode := interceptor.getStatusCode()

			isDisconnected := false
			if r.Context().Err() != nil || (statusCode == http.StatusOK && interceptor.bytesWritten == 0 && r.Method == http.MethodGet) {

				if errors.Is(r.Context().Err(), context.Canceled) {
					isDisconnected = true
				}
			}

			var logEvent *zerolog.Event
			switch {
			case statusCode >= 500:
				logEvent = logger.Error()
			case statusCode >= 400:
				logEvent = logger.Warn()
			case isDisconnected:
				logEvent = logger.Debug()
			default:
				logEvent = logger.Info()
			}

			event := logEvent.Str("method", r.Method).
				Str("path", r.URL.Path).
				Str("proto", r.Proto).
				Int("status", statusCode).
				Int("size_bytes", interceptor.bytesWritten).
				Str("duration", util.FormatDuration(duration))

			if remoteAddr := r.RemoteAddr; remoteAddr != "" {
				event = event.Str("remote_addr", remoteAddr)
			}
			if userAgent := r.Header.Get("User-Agent"); userAgent != "" {
				event = event.Str("user_agent", userAgent)
			}
			if referer := r.Header.Get("Referer"); referer != "" {
				event = event.Str("referer", referer)
			}
			if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
				event = event.Str("x_forwarded_for", xff)
			}
			if cacheStatus := w.Header().Get("X-Cache-Status"); cacheStatus != "" {
				event = event.Str("cache_status", cacheStatus)
			}

			msg := "Request handled"
			if isDisconnected {
				msg = "Client disconnected"
			}
			event.Msg(msg)
		})
	}
}

func RecoveryMiddleware(logger zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {

					if !headersWritten(w) {
						http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
					}

					logger.Error().
						Interface("panic_value", err).
						Bytes("stack_trace", debug.Stack()).
						Str("request_uri", r.RequestURI).
						Msg("Panic recovered in HTTP handler")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func headersWritten(w http.ResponseWriter) bool {

	if wi, ok := w.(*responseWriterInterceptor); ok {
		return wi.statusCode != 0
	}

	return false
}
