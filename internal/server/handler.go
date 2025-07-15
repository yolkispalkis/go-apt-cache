package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
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

func (app *Application) handleServeRepoContent(w http.ResponseWriter, r *http.Request) {
	repo := r.Context().Value(repoContextKey).(config.Repository)
	relPath := chi.URLParam(r, "*")

	cleanRelPath := filepath.Clean(relPath)

	if strings.HasPrefix(cleanRelPath, "..") || cleanRelPath == ".." || cleanRelPath == "." {
		app.Logger.Warn().
			Str("repo", repo.Name).
			Str("remote_addr", r.RemoteAddr).
			Str("raw_path", relPath).
			Msg("Path traversal attempt blocked")
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	key := repo.Name + "/" + cleanRelPath
	upstreamURL := repo.URL + cleanRelPath
	log := app.Logger.With().Str("key", key).Str("repo", repo.Name).Logger()

	meta, found := app.Cache.Get(r.Context(), key)
	if found {
		log.Debug().Msg("Cache hit")
		if meta.StatusCode == http.StatusNotFound {
			if meta.IsStale(time.Now()) {
				log.Info().Msg("Stale negative cache entry, re-fetching")
				app.Cache.Delete(r.Context(), key)
				app.fetchAndServe(w, r, key, upstreamURL, nil, cleanRelPath)
			} else {
				log.Debug().Msg("Serving 404 from negative cache")
				util.CopyWhitelistedHeaders(w.Header(), meta.Headers) // Добавлено
				http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			}
			return
		}

		if meta.IsStale(time.Now()) || r.Header.Get("Cache-Control") == "no-cache" {
			log.Info().Msg("Revalidating stale/no-cache item")
			app.revalidate(w, r, key, upstreamURL, meta, cleanRelPath)
		} else {
			app.serveFromCache(w, r, key, meta, cleanRelPath)
		}
		return
	}

	log.Info().Msg("Cache miss, fetching from upstream")
	app.fetchAndServe(w, r, key, upstreamURL, nil, cleanRelPath)
}

func (app *Application) serveFromCache(w http.ResponseWriter, r *http.Request, key string, meta *cache.ItemMeta, cleanRelPath string) {
	if util.CheckConditional(w, r, meta.Headers) {
		return
	}

	if r.Context().Value("refetching") != nil { // Guard
		app.Logger.Error().Str("key", key).Msg("Refetch loop detected, aborting")
		http.Error(w, "Internal cache error", http.StatusInternalServerError)
		return
	}
	ctx := context.WithValue(r.Context(), "refetching", true)

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
			app.Cache.Delete(ctx, key)
			app.fetchAndServe(w, r.WithContext(ctx), key, meta.UpstreamURL, nil, cleanRelPath) // Use ctx
			return
		}
		app.Logger.Error().Err(err).Str("key", key).Msg("Failed to get content for cached metadata")
		http.Error(w, "Cache content unavailable", http.StatusInternalServerError)
		return
	}
	defer content.Close()

	w.WriteHeader(meta.StatusCode)

	buf := util.GetBuffer()
	defer util.ReturnBuffer(buf)
	_, err = io.CopyBuffer(w, content, buf) // Изменено на CopyBuffer
	if err != nil {
		app.Logger.Warn().Err(err).Str("key", key).Msg("Error copying cached content to response")
	}
}

func (app *Application) fetchAndServe(w http.ResponseWriter, r *http.Request, key, upstreamURL string, revalMeta *cache.ItemMeta, relPath string) {
	opts := &fetch.Options{}
	if revalMeta != nil {
		opts.IfNoneMatch = revalMeta.Headers.Get("ETag")
		if t, err := http.ParseTime(revalMeta.Headers.Get("Last-Modified")); err == nil {
			opts.IfModSince = t
		}
	}

	resInterface, _, shared := app.Fetcher.Fetch(r.Context(), key, upstreamURL, opts)

	sharedFetch, ok := resInterface.(*fetch.SharedFetch)
	if !ok {
		if resInterface == nil {
			app.Logger.Warn().Str("key", key).Msg("Fetch initiation failed, possibly due to client disconnect.")
			return
		}
		app.Logger.Error().Str("key", key).Msgf("Unexpected type from singleflight: %T", resInterface)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if shared {
		app.Logger.Debug().Str("key", key).Msg("Following a shared fetch, waiting for leader.")
		select {
		case <-sharedFetch.Done:
			app.Logger.Debug().Str("key", key).Msg("Leader finished, proceeding to serve from cache.")
			meta, found := app.Cache.Get(r.Context(), key)
			if !found {
				app.Logger.Error().Str("key", key).Msg("Leader was supposed to cache the item, but it's not found.")
				http.Error(w, "Failed to retrieve file after fetch", http.StatusInternalServerError)
				return
			}
			app.serveFromCache(w, r, key, meta, relPath)
		case <-r.Context().Done():
			app.Logger.Info().Str("key", key).Msg("Client disconnected while waiting for leader.")
			return
		}
		return
	}

	fetchRes := sharedFetch.Result
	err := sharedFetch.Err

	defer close(sharedFetch.Done) // Перенесено сюда, перед обработкой ошибок

	if fetchRes != nil && fetchRes.Header != nil {
		defer util.ReturnHeader(fetchRes.Header)
	}
	if fetchRes != nil && fetchRes.Body != nil {
		defer fetchRes.Body.Close()
	}

	if err != nil {
		if errors.Is(err, fetch.ErrUpstreamNotFound) {
			app.Logger.Info().Str("key", key).Msg("Upstream returned 404 Not Found")
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
			util.CopyWhitelistedHeaders(w.Header(), fetchRes.Header) // Добавлено: копируем заголовки
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}

		if errors.Is(err, fetch.ErrUpstreamNotModified) {
			app.Logger.Info().Str("key", key).Msg("Revalidation successful (304)")
			if revalMeta != nil {
				updatedMeta := *revalMeta
				updatedMeta.Headers = util.CopyHeader(revalMeta.Headers)

				updatedMeta.ExpiresAt = calculateFreshness(fetchRes.Header, time.Now(), relPath, app.Config.Cache.Overrides)
				updatedMeta.LastUsedAt = time.Now()
				util.UpdateCacheHeaders(updatedMeta.Headers, fetchRes.Header)

				if err := app.Cache.Put(r.Context(), &updatedMeta); err != nil {
					app.Logger.Error().Err(err).Str("key", key).Msg("Failed to update metadata after 304")
					util.ReturnHeader(updatedMeta.Headers)
				}
				app.serveFromCache(w, r, key, &updatedMeta, relPath)
			}
			return
		}

		app.Logger.Warn().Err(err).Str("key", key).Msg("Failed to fetch from upstream")
		http.Error(w, "Upstream fetch failed", http.StatusBadGateway)
		return
	}

	if util.CheckConditional(w, r, fetchRes.Header) {
		return
	}

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

	cacheCtx := r.Context()

	go func() {
		defer wg.Done()
		written, err := app.Cache.PutContent(cacheCtx, key, pr)
		if err != nil {
			if !util.IsClientDisconnectedError(err) {
				app.Logger.Error().Err(err).Str("key", key).Msg("Failed to write content to cache")
			}
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

func (app *Application) revalidate(w http.ResponseWriter, r *http.Request, key, upstreamURL string, currentMeta *cache.ItemMeta, relPath string) {
	app.fetchAndServe(w, r, key, upstreamURL, currentMeta, relPath)
}

func (app *Application) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	var sb strings.Builder
	sb.WriteString("OK\n")
	sb.WriteString("Cache status endpoint is active.\n")
	w.Write([]byte(sb.String()))
}

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
