package server

import (
	"context"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type contextKey string

const (
	repoContextKey    = contextKey("repo")
	loggerContextKey  = contextKey("logger")
	configContextKey  = contextKey("config")
	cacheContextKey   = contextKey("cache")
	fetcherContextKey = contextKey("fetcher")
)

func (app *Application) injectDependencies(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		ctx = context.WithValue(ctx, loggerContextKey, app.Logger)
		ctx = context.WithValue(ctx, configContextKey, app.Config)
		ctx = context.WithValue(ctx, cacheContextKey, app.Cache)
		ctx = context.WithValue(ctx, fetcherContextKey, app.Fetcher)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger := r.Context().Value(loggerContextKey).(*logging.Logger)
		start := time.Now()
		ww := util.NewResponseWriterInterceptor(w)
		next.ServeHTTP(ww, r)

		logger.Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", ww.Status()).
			Int64("bytes", ww.BytesWritten()).
			Dur("duration", time.Since(start)).
			Str("remote_addr", r.RemoteAddr).
			Msg("Request handled")
	})
}

func recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger := r.Context().Value(loggerContextKey).(*logging.Logger)
		defer func() {
			if err := recover(); err != nil {
				w.Header().Set("Connection", "close")
				logger.Error().
					Interface("panic", err).
					Bytes("stack", debug.Stack()).
					Msg("Panic recovered")
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func repoContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := r.Context().Value(configContextKey).(*config.Config)
		repoName := chi.URLParam(r, "repoName")
		repo, ok := cfg.GetRepo(repoName)
		if !ok {
			http.NotFound(w, r)
			return
		}
		ctx := context.WithValue(r.Context(), repoContextKey, repo)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
