package server

import (
	"net/http"
	"runtime/debug"
	"time"

	"github.com/rs/zerolog"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type respWriterInterceptor struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *respWriterInterceptor) WriteHeader(statusCode int) {
	if w.status == 0 {
		w.status = statusCode
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *respWriterInterceptor) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

func (w *respWriterInterceptor) getStatus() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func LoggingMiddleware(log zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			iw := &respWriterInterceptor{ResponseWriter: w}

			next.ServeHTTP(iw, r)

			duration := time.Since(start)
			statusCode := iw.getStatus()

			var logEvent *zerolog.Event
			clientDisconnected := util.IsClientDisconnectedError(r.Context().Err())

			switch {
			case statusCode >= 500:
				logEvent = log.Error()
			case statusCode >= 400:
				logEvent = log.Warn()
			case clientDisconnected:

				logEvent = log.Debug()
			default:
				logEvent = log.Info()
			}

			event := logEvent.Str("meth", r.Method).
				Str("path", r.URL.Path).
				Str("proto", r.Proto).
				Int("status", statusCode).
				Int64("bytes", iw.bytes).
				Str("dur", util.FormatDuration(duration))

			if r.RemoteAddr != "" {
				event = event.Str("remote", r.RemoteAddr)
			}
			if ua := r.UserAgent(); ua != "" {
				event = event.Str("ua", ua)
			}
			if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
				event = event.Str("xff", xff)
			}
			if cStat := w.Header().Get("X-Cache-Status"); cStat != "" {
				event = event.Str("cache", cStat)
			}

			msg := "Request"
			if clientDisconnected {
				msg = "Client disconnected"
			}
			event.Msg(msg)
		})
	}
}

func RecoveryMiddleware(log zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {

					headersSent := false
					if rwi, ok := w.(*respWriterInterceptor); ok {
						headersSent = rwi.status != 0
					} else {

					}

					if !headersSent {
						http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
					}

					log.Error().
						Interface("panic_value", rec).
						Bytes("stack_trace", debug.Stack()).
						Str("request_uri", r.RequestURI).
						Msg("Panic recovered in HTTP handler")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
