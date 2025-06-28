package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/fetch"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

// handleServeRepoContent - основной обработчик запросов к репозиториям.
func (app *Application) handleServeRepoContent(w http.ResponseWriter, r *http.Request) {
	repo := r.Context().Value(repoContextKey).(config.Repository)
	relPath := chi.URLParam(r, "*")
	key := repo.Name + "/" + relPath
	upstreamURL := repo.URL + relPath
	log := app.Logger.With("key", key, "repo", repo.Name)

	// 1. Проверка кеша метаданных.
	meta, found := app.Cache.Get(r.Context(), key)
	if found {
		log.Debug("Cache hit for metadata")
		if meta.IsStale(time.Now()) || r.Header.Get("Cache-Control") == "no-cache" {
			log.Info("Revalidating stale/no-cache item")
			app.revalidate(w, r, key, upstreamURL, meta)
		} else {
			app.serveFromCache(w, r, key, meta)
		}
		return
	}

	// 2. Кеш промахнулся, загружаем с апстрима.
	log.Info("Cache miss, fetching from upstream")
	app.fetchAndServe(w, r, key, upstreamURL, nil)
}

// serveFromCache отдает ответ, используя закешированные данные.
func (app *Application) serveFromCache(w http.ResponseWriter, r *http.Request, key string, meta *cache.ItemMeta) {
	if util.CheckConditional(w, r, meta.Headers) {
		return // 304 Not Modified
	}

	util.CopyWhitelistedHeaders(w.Header(), meta.Headers)
	w.Header().Set("X-Cache-Status", "HIT")

	if r.Method == http.MethodHead {
		w.WriteHeader(meta.StatusCode)
		return
	}

	content, err := app.Cache.GetContent(r.Context(), key)
	if err != nil {
		app.Logger.Error("Failed to get content for cached metadata", "key", key, "error", err)
		http.Error(w, "Cache content unavailable", http.StatusInternalServerError)
		return
	}
	defer content.Close()

	w.WriteHeader(meta.StatusCode)
	io.Copy(w, content)
}

// fetchAndServe загружает данные с апстрима, отдает клиенту и кеширует.
func (app *Application) fetchAndServe(w http.ResponseWriter, r *http.Request, key, upstreamURL string, revalMeta *cache.ItemMeta) {
	opts := &fetch.Options{}
	if revalMeta != nil {
		opts.IfNoneMatch = revalMeta.Headers.Get("ETag")
		if t, err := http.ParseTime(revalMeta.Headers.Get("Last-Modified")); err == nil {
			opts.IfModSince = t
		}
	}

	fetchRes, err := app.Fetcher.Fetch(r.Context(), key, upstreamURL, opts)
	if fetchRes != nil && fetchRes.Body != nil {
		defer fetchRes.Body.Close()
	}

	if err != nil {
		if errors.Is(err, fetch.ErrUpstreamNotModified) {
			app.Logger.Info("Revalidation successful (304)", "key", key)
			if revalMeta != nil {
				relPath := strings.TrimPrefix(key, revalMeta.Key)
				revalMeta.ExpiresAt = util.CalculateFreshness(fetchRes.Header, time.Now(), relPath, app.Config.Cache.Overrides)
				util.UpdateCacheHeaders(revalMeta.Headers, fetchRes.Header)
				app.Cache.Put(r.Context(), key, revalMeta)
				app.serveFromCache(w, r, key, revalMeta)
			}
			return
		}
		app.Logger.Warn("Failed to fetch from upstream", "key", key, "error", err)
		http.Error(w, "Upstream fetch failed", http.StatusBadGateway)
		return
	}

	if util.CheckConditional(w, r, fetchRes.Header) {
		return
	}

	relPath := strings.TrimPrefix(key, chi.URLParam(r, "repoName")+"/")
	meta := &cache.ItemMeta{
		Key:         key,
		UpstreamURL: upstreamURL,
		FetchedAt:   time.Now(),
		StatusCode:  fetchRes.Status,
		Headers:     util.CopyHeader(fetchRes.Header),
		Size:        fetchRes.Size,
		ExpiresAt:   util.CalculateFreshness(fetchRes.Header, time.Now(), relPath, app.Config.Cache.Overrides),
	}

	util.CopyWhitelistedHeaders(w.Header(), meta.Headers)
	w.Header().Set("X-Cache-Status", "MISS")

	if r.Method == http.MethodHead {
		w.WriteHeader(meta.StatusCode)
		app.Cache.Put(r.Context(), key, meta)
		return
	}

	pr, pw := io.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		written, err := app.Cache.PutContent(context.Background(), key, pr)
		if err != nil {
			app.Logger.Error("Failed to write content to cache", "key", key, "error", err)
			app.Cache.DeleteContent(context.Background(), key)
		} else {
			meta.Size = written
			app.Cache.Put(context.Background(), key, meta)
		}
	}()

	tee := io.TeeReader(fetchRes.Body, pw)
	w.WriteHeader(meta.StatusCode)
	_, copyErr := io.Copy(w, tee)

	if copyErr != nil {
		pw.CloseWithError(copyErr)
	} else {
		pw.Close()
	}

	wg.Wait()
}

// revalidate выполняет условный запрос к апстриму.
func (app *Application) revalidate(w http.ResponseWriter, r *http.Request, key, upstreamURL string, currentMeta *cache.ItemMeta) {
	app.fetchAndServe(w, r, key, upstreamURL, currentMeta)
}

func (app *Application) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	var sb strings.Builder
	sb.WriteString("OK\n")

	if app.Config.Cache.Enabled {
		stats := app.Cache.Stats()
		fmt.Fprintf(&sb, "Cache Hits: %d\n", stats.Hits())
		fmt.Fprintf(&sb, "Cache Misses: %d\n", stats.Misses())
		fmt.Fprintf(&sb, "Cache Hit Ratio: %.2f\n", stats.Ratio())
	} else {
		sb.WriteString("Cache: Disabled\n")
	}

	w.Write([]byte(sb.String()))
}
