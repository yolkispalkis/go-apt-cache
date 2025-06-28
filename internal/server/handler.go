package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bmatcuk/doublestar/v4"
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
	log := app.Logger.With().Str("key", key).Str("repo", repo.Name).Logger()

	// 1. Проверка кеша.
	meta, found := app.Cache.Get(r.Context(), key)
	if found {
		log.Debug().Msg("Cache hit")
		// Обработка негативного кеша (404).
		if meta.StatusCode == http.StatusNotFound {
			if meta.IsStale(time.Now()) {
				log.Info().Msg("Stale negative cache entry, re-fetching")
				app.Cache.Delete(context.Background(), key)
				app.fetchAndServe(w, r, key, upstreamURL, nil)
			} else {
				log.Debug().Msg("Serving 404 from negative cache")
				http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			}
			return
		}

		// Обработка обычного кеша.
		if meta.IsStale(time.Now()) || r.Header.Get("Cache-Control") == "no-cache" {
			log.Info().Msg("Revalidating stale/no-cache item")
			app.revalidate(w, r, key, upstreamURL, meta)
		} else {
			app.serveFromCache(w, r, key, meta)
		}
		return
	}

	// 2. Кеш промахнулся, загружаем с апстрима.
	log.Info().Msg("Cache miss, fetching from upstream")
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
		if errors.Is(err, os.ErrNotExist) {
			app.Logger.Warn().Str("key", key).Msg("Metadata found, but content is missing. Refetching.")
			app.Cache.Delete(context.Background(), key) // Удаляем "сломанную" запись
			app.fetchAndServe(w, r, key, meta.UpstreamURL, nil)
			return
		}
		app.Logger.Error().Err(err).Str("key", key).Msg("Failed to get content for cached metadata")
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
		// Обработка 404 ошибки
		if errors.Is(err, fetch.ErrUpstreamNotFound) {
			app.Logger.Info().Str("key", key).Msg("Upstream returned 404 Not Found")
			// Кешируем 404 ответ, если настроено
			if app.Config.Cache.NegativeTTL > 0 {
				now := time.Now()
				meta := &cache.ItemMeta{
					Key:         key,
					UpstreamURL: upstreamURL,
					FetchedAt:   now,
					LastUsedAt:  now,
					StatusCode:  http.StatusNotFound,
					Headers:     util.CopyHeader(fetchRes.Header),
					Size:        0,
					ExpiresAt:   now.Add(app.Config.Cache.NegativeTTL),
				}
				if err := app.Cache.Put(r.Context(), meta); err != nil {
					app.Logger.Error().Err(err).Str("key", key).Msg("Failed to save negative cache entry")
				}
			}
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}

		// Обработка 304 Not Modified
		if errors.Is(err, fetch.ErrUpstreamNotModified) {
			app.Logger.Info().Str("key", key).Msg("Revalidation successful (304)")
			if revalMeta != nil {
				relPath := strings.TrimPrefix(key, revalMeta.UpstreamURL)
				revalMeta.ExpiresAt = calculateFreshness(fetchRes.Header, time.Now(), relPath, app.Config.Cache.Overrides)
				revalMeta.LastUsedAt = time.Now()
				util.UpdateCacheHeaders(revalMeta.Headers, fetchRes.Header)
				if err := app.Cache.Put(r.Context(), revalMeta); err != nil {
					app.Logger.Error().Err(err).Str("key", key).Msg("Failed to update metadata after 304")
				}
				app.serveFromCache(w, r, key, revalMeta)
			}
			return
		}

		// Обработка других ошибок
		app.Logger.Warn().Err(err).Str("key", key).Msg("Failed to fetch from upstream")
		http.Error(w, "Upstream fetch failed", http.StatusBadGateway)
		return
	}

	if util.CheckConditional(w, r, fetchRes.Header) {
		return
	}

	relPath := strings.TrimPrefix(key, chi.URLParam(r, "repoName")+"/")
	now := time.Now()
	meta := &cache.ItemMeta{
		Key:         key,
		UpstreamURL: upstreamURL,
		FetchedAt:   now,
		LastUsedAt:  now,
		StatusCode:  fetchRes.Status,
		Headers:     util.CopyHeader(fetchRes.Header),
		Size:        fetchRes.Size,
		ExpiresAt:   calculateFreshness(fetchRes.Header, now, relPath, app.Config.Cache.Overrides),
	}

	util.CopyWhitelistedHeaders(w.Header(), meta.Headers)
	w.Header().Set("X-Cache-Status", "MISS")

	if r.Method == http.MethodHead {
		w.WriteHeader(meta.StatusCode)
		if err := app.Cache.Put(r.Context(), meta); err != nil {
			app.Logger.Error().Err(err).Str("key", key).Msg("Failed to save metadata for HEAD request")
		}
		return
	}

	pr, pw := io.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		written, err := app.Cache.PutContent(context.Background(), key, pr)
		if err != nil {
			app.Logger.Error().Err(err).Str("key", key).Msg("Failed to write content to cache")
			app.Cache.Delete(context.Background(), key)
		} else {
			meta.Size = written
			if err := app.Cache.Put(context.Background(), meta); err != nil {
				app.Logger.Error().Err(err).Str("key", key).Msg("Failed to save metadata after content write")
			}
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
	sb.WriteString("Cache status endpoint is active.\n")
	w.Write([]byte(sb.String()))
}

// --- Логика вычисления свежести ---

func calculateFreshness(headers http.Header, responseTime time.Time, relPath string, overrides []config.CacheOverride) time.Time {
	if overrideTTL, ok := findOverrideTTL(relPath, overrides); ok {
		return responseTime.Add(overrideTTL)
	}

	cc := util.ParseCacheControl(headers.Get("Cache-Control"))

	if _, ok := cc["no-store"]; ok {
		return time.Time{}
	}
	if _, ok := cc["no-cache"]; ok {
		return responseTime
	}

	var lifetime time.Duration
	if sMaxAge, ok := cc["s-maxage"]; ok {
		if sec, err := strconv.ParseInt(sMaxAge, 10, 64); err == nil {
			lifetime = time.Duration(sec) * time.Second
		}
	} else if maxAge, ok := cc["max-age"]; ok {
		if sec, err := strconv.ParseInt(maxAge, 10, 64); err == nil {
			lifetime = time.Duration(sec) * time.Second
		}
	} else if expiresStr := headers.Get("Expires"); expiresStr != "" {
		if expires, err := http.ParseTime(expiresStr); err == nil {
			lifetime = expires.Sub(responseTime)
		}
	} else if lmStr := headers.Get("Last-Modified"); lmStr != "" {
		if lm, err := http.ParseTime(lmStr); err == nil {
			lifetime = responseTime.Sub(lm) / 10
		}
	}

	if lifetime < 0 {
		lifetime = 0
	}

	return responseTime.Add(lifetime)
}

func findOverrideTTL(relPath string, overrides []config.CacheOverride) (time.Duration, bool) {
	for _, rule := range overrides {
		matched, err := doublestar.Match(rule.PathPattern, relPath)
		if err == nil && matched {
			return rule.TTL, true
		}
	}
	return 0, false
}
