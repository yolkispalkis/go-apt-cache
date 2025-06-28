package server

import (
	"context"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type contextKey string

const repoContextKey = contextKey("repo")

// logRequest - middleware для логирования каждого запроса.
func (app *Application) logRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := util.NewResponseWriterInterceptor(w)
		next.ServeHTTP(ww, r)

		app.Logger.Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", ww.Status()).
			Int64("bytes", ww.BytesWritten()).
			Dur("duration", time.Since(start)).
			Str("remote_addr", r.RemoteAddr).
			Msg("Request handled")
	})
}

// recoverPanic - middleware для восстановления после паник.
func (app *Application) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				w.Header().Set("Connection", "close")
				app.Logger.Error().
					Interface("panic", err).
					Bytes("stack", debug.Stack()).
					Msg("Panic recovered")
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// repoContext - middleware для проверки существования репозитория и добавления его в контекст.
func (app *Application) repoContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		repoName := chi.URLParam(r, "repoName")
		repo, ok := app.Config.GetRepo(repoName)
		if !ok {
			http.NotFound(w, r)
			return
		}
		ctx := context.WithValue(r.Context(), repoContextKey, repo)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
