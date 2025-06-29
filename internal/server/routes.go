package server

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (app *Application) Routes() http.Handler {
	r := chi.NewRouter()

	r.Use(app.recoverPanic)
	r.Use(app.logRequest)

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "Go APT Cache operational.")
	})
	r.Get("/status", app.handleStatus)

	r.Route("/{repoName}", func(r chi.Router) {
		r.Use(app.repoContext)
		r.Get("/*", app.handleServeRepoContent)
		r.Head("/*", app.handleServeRepoContent)
	})

	return r
}
