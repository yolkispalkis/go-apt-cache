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
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

const (
	cacheStatusHit  = "HIT"
	cacheStatusMiss = "MISS"
	refetchKey      = "refetching" // Context key for loop guard
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
	log := app.Logger.WithContext("key", key, "repo", repo.Name)

	meta, found := app.Cache.Get(r.Context(), key)
	if !found {
		log.Info().Msg("Cache miss, fetching from upstream")
		app.fetchAndServe(w, r, key, upstreamURL, nil, cleanRelPath, log)
		return
	}
	defer util.ReturnHeader(meta.Headers)

	log.Debug().Msg("Cache hit")
	if meta.StatusCode == http.StatusNotFound {
		handleNegativeCache(app, w, r, key, upstreamURL, meta, cleanRelPath, log)
		return
	}

	if meta.IsStale(time.Now()) {
		log.Info().Msg("Revalidating stale item")
		app.fetchAndServe(w, r, key, upstreamURL, meta, cleanRelPath, log)
		return
	}

	app.serveFromCache(w, r, key, meta, cleanRelPath, log)
}

func handleNegativeCache(app *Application, w http.ResponseWriter, r *http.Request, key, upstreamURL string, meta *cache.ItemMeta, cleanRelPath string, log *logging.Logger) {
	if meta.IsStale(time.Now()) {
		log.Info().Msg("Stale negative cache entry, re-fetching")
		app.Cache.Delete(r.Context(), key)
		app.fetchAndServe(w, r, key, upstreamURL, nil, cleanRelPath, log)
		return
	}

	log.Debug().Msg("Serving 404 from negative cache")
	util.CopyWhitelistedHeaders(w.Header(), meta.Headers)
	http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

func (app *Application) serveFromCache(w http.ResponseWriter, r *http.Request, key string, meta *cache.ItemMeta, cleanRelPath string, log *logging.Logger) {
	log.Debug().
		Str("client_if_none_match", r.Header.Get("If-None-Match")).
		Str("client_if_modified_since", r.Header.Get("If-Modified-Since")).
		Str("cached_etag", meta.Headers.Get("ETag")).
		Str("cached_last_modified", meta.Headers.Get("Last-Modified")).
		Msg("Checking conditional request")

	if util.CheckConditional(w, r, meta.Headers) {
		log.Debug().Msg("Conditional check succeeded, returning 304")
		return
	}
	log.Debug().Msg("Conditional check failed, serving full response")

	if r.Context().Value(refetchKey) != nil {
		log.Error().Msg("Refetch loop detected, aborting")
		http.Error(w, "Internal cache error", http.StatusInternalServerError)
		return
	}
	ctx := context.WithValue(r.Context(), refetchKey, true)

	util.CopyWhitelistedHeaders(w.Header(), meta.Headers)
	w.Header().Set("X-Cache-Status", cacheStatusHit)

	if r.Method == http.MethodHead {
		w.WriteHeader(meta.StatusCode)
		return
	}

	content, err := app.Cache.GetContent(ctx, key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Warn().Msg("Metadata found, but content is missing. Refetching.")
			app.Cache.Delete(ctx, key)
			app.fetchAndServe(w, r.WithContext(ctx), key, meta.UpstreamURL, nil, cleanRelPath, log)
			return
		}
		log.Error().Err(err).Msg("Failed to get content for cached metadata")
		http.Error(w, "Cache content unavailable", http.StatusInternalServerError)
		return
	}
	defer content.Close()

	w.WriteHeader(meta.StatusCode)

	buf := util.GetBuffer()
	defer util.ReturnBuffer(buf)
	if _, copyErr := io.CopyBuffer(w, content, buf); copyErr != nil {
		log.Warn().Err(copyErr).Msg("Error copying cached content to response")
	}
}

func (app *Application) fetchAndServe(w http.ResponseWriter, r *http.Request, key, upstreamURL string, revalMeta *cache.ItemMeta, relPath string, log *logging.Logger) {
	opts := extractOptions(r, revalMeta)
	resInterface, _, shared := app.Fetcher.Fetch(r.Context(), key, upstreamURL, opts)

	sharedFetch, ok := resInterface.(*fetch.SharedFetch)
	if !ok {
		handleInvalidFetchType(log, key, resInterface, w)
		return
	}

	handleFetchResult(app, w, r, key, upstreamURL, relPath, sharedFetch, shared, revalMeta, log)
}

func extractOptions(r *http.Request, revalMeta *cache.ItemMeta) *fetch.Options {
	if revalMeta != nil {
		opts := &fetch.Options{IfNoneMatch: revalMeta.Headers.Get("ETag")}
		if t, err := http.ParseTime(revalMeta.Headers.Get("Last-Modified")); err == nil {
			opts.IfModSince = t
		}
		return opts
	}

	opts := &fetch.Options{IfNoneMatch: r.Header.Get("If-None-Match")}
	if t, err := http.ParseTime(r.Header.Get("If-Modified-Since")); err == nil {
		opts.IfModSince = t
	}
	return opts
}

func handleInvalidFetchType(log *logging.Logger, key string, resInterface any, w http.ResponseWriter) {
	if resInterface == nil {
		log.Warn().Str("key", key).Msg("Fetch initiation failed, possibly due to client disconnect.")
		return
	}
	log.Error().Str("key", key).Msgf("Unexpected type from singleflight: %T", resInterface)
	http.Error(w, "Internal server error", http.StatusInternalServerError)
}

func handleFetchResult(app *Application, w http.ResponseWriter, r *http.Request, key, upstreamURL, relPath string, sharedFetch *fetch.SharedFetch, shared bool, revalMeta *cache.ItemMeta, log *logging.Logger) {
	if shared {
		handleSharedFetch(app, w, r, key, relPath, sharedFetch, log)
		return
	}

	fetchRes := sharedFetch.Result
	err := sharedFetch.Err

	if fetchRes != nil {
		if fetchRes.Header != nil {
			defer util.ReturnHeader(fetchRes.Header)
		}
		if fetchRes.Body != nil {
			defer fetchRes.Body.Close()
		}
	}

	if err != nil {
		handleFetchError(app, w, r, key, upstreamURL, relPath, err, fetchRes, revalMeta, log)
		return
	}

	handleFetchSuccess(app, w, r, key, upstreamURL, relPath, fetchRes, log)
}

func handleSharedFetch(app *Application, w http.ResponseWriter, r *http.Request, key, relPath string, sharedFetch *fetch.SharedFetch, log *logging.Logger) {
	log.Debug().Msg("Shared fetch completed, serving from cache.")

	if sharedFetch.Err != nil {
		log.Warn().Err(sharedFetch.Err).Msg("Shared fetch failed")
		http.Error(w, "Upstream fetch failed", http.StatusBadGateway)
		return
	}

	meta, found := app.Cache.Get(r.Context(), key)
	if !found {
		log.Error().Msg("Item not found in cache after shared fetch")
		http.Error(w, "Failed to retrieve file after fetch", http.StatusInternalServerError)
		return
	}
	defer util.ReturnHeader(meta.Headers)
	app.serveFromCache(w, r, key, meta, relPath, log)
}

func handleFetchError(app *Application, w http.ResponseWriter, r *http.Request, key, upstreamURL, relPath string, err error, fetchRes *fetch.Result, revalMeta *cache.ItemMeta, log *logging.Logger) {
	if errors.Is(err, fetch.ErrUpstreamNotFound) {
		log.Info().Err(err).Msg("Upstream returned 404 Not Found")
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
			if putErr := app.Cache.Put(r.Context(), meta); putErr != nil {
				log.Error().Err(putErr).Msg("Failed to save negative cache entry")
			}
		}
		util.CopyWhitelistedHeaders(w.Header(), fetchRes.Header)
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	if errors.Is(err, fetch.ErrUpstreamNotModified) {
		log.Info().Err(err).Msg("Revalidation successful (304)")
		if revalMeta == nil {
			log.Warn().Msg("Upstream returned 304 on a cache miss. Forwarding to client, but unable to cache.")
			util.CopyWhitelistedHeaders(w.Header(), fetchRes.Header)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		updatedMeta := *revalMeta
		updatedMeta.Headers = util.CopyHeader(revalMeta.Headers)
		updatedMeta.ExpiresAt = calculateFreshness(fetchRes.Header, time.Now(), relPath, app.Config.Cache.Overrides)
		updatedMeta.LastUsedAt = time.Now()
		util.UpdateCacheHeaders(updatedMeta.Headers, fetchRes.Header)

		if putErr := app.Cache.Put(r.Context(), &updatedMeta); putErr != nil {
			log.Error().Err(putErr).Msg("Failed to update metadata after 304")
			util.ReturnHeader(updatedMeta.Headers)
			return
		}
		app.serveFromCache(w, r, key, &updatedMeta, relPath, log)
		return
	}

	log.Warn().Err(err).Msg("Failed to fetch from upstream")
	http.Error(w, "Upstream fetch failed", http.StatusBadGateway)
}

func handleFetchSuccess(app *Application, w http.ResponseWriter, r *http.Request, key, upstreamURL, relPath string, fetchRes *fetch.Result, log *logging.Logger) {
	now := time.Now()
	headersCopy := util.CopyHeader(fetchRes.Header)

	meta := &cache.ItemMeta{
		Key:         key,
		UpstreamURL: upstreamURL,
		FetchedAt:   now,
		LastUsedAt:  now,
		StatusCode:  fetchRes.Status,
		Headers:     headersCopy,
		Size:        fetchRes.Size,
		ExpiresAt:   calculateFreshness(headersCopy, now, relPath, app.Config.Cache.Overrides),
	}

	util.CopyWhitelistedHeaders(w.Header(), meta.Headers)
	w.Header().Set("X-Cache-Status", cacheStatusMiss)

	if r.Method == http.MethodHead {
		w.WriteHeader(meta.StatusCode)
		if err := app.Cache.Put(r.Context(), meta); err != nil {
			log.Error().Err(err).Msg("Failed to save metadata for HEAD request")
		}
		return
	}

	pr, pw := io.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)
	var copyErr error

	go func(m *cache.ItemMeta) {
		defer wg.Done()
		written, err := app.Cache.PutContent(r.Context(), key, pr)
		if err != nil {
			if !util.IsClientDisconnectedError(err) {
				log.Error().Err(err).Msg("Failed to write content to cache")
			}
			app.Cache.Delete(context.Background(), key)
			return
		}
		m.Size = written
		if err := app.Cache.Put(context.Background(), m); err != nil {
			log.Error().Err(err).Msg("Failed to save metadata after content write")
		}
	}(meta)

	tee := io.TeeReader(fetchRes.Body, pw)

	if util.CheckConditional(w, r, meta.Headers) {
		log.Debug().Msg("Client has fresh version, sending 304 and caching in background.")
		_, copyErr = io.Copy(io.Discard, tee)
	} else {
		w.WriteHeader(meta.StatusCode)
		_, copyErr = io.Copy(w, tee)
	}

	if copyErr != nil {
		pw.CloseWithError(copyErr)
	} else {
		pw.Close()
	}

	wg.Wait()
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
