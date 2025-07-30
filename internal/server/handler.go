package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
)

type requestHandler struct {
	w   http.ResponseWriter
	r   *http.Request
	log *logging.Logger

	cache   cache.Manager
	fetcher *fetch.Coordinator
	cfg     *config.Config

	repo         config.Repository
	key          string
	upstreamURL  string
	cleanRelPath string
}

func (app *Application) newRequestHandler(w http.ResponseWriter, r *http.Request) (*requestHandler, error) {
	repo := r.Context().Value(repoContextKey).(config.Repository)
	relPath := chi.URLParam(r, "*")

	/* двойное URL-раскодирование → защита от %252e%252e etc. */
	if un, err := url.PathUnescape(relPath); err == nil {
		relPath = un
		if un2, err2 := url.PathUnescape(relPath); err2 == nil {
			relPath = un2
		}
	}

	cleanRelPath := filepath.Clean(relPath)

	if strings.HasPrefix(cleanRelPath, "..") || cleanRelPath == ".." || cleanRelPath == "." {
		app.Logger.Warn().
			Str("repo", repo.Name).
			Str("remote_addr", r.RemoteAddr).
			Str("raw_path", relPath).
			Msg("Path traversal attempt blocked")
		return nil, errors.New("invalid path")
	}

	key := repo.Name + "/" + cleanRelPath
	h := &requestHandler{
		w:            w,
		r:            r,
		log:          app.Logger.WithContext("key", key, "repo", repo.Name),
		cache:        app.Cache,
		fetcher:      app.Fetcher,
		cfg:          app.Config,
		repo:         repo,
		key:          key,
		upstreamURL:  repo.URL + cleanRelPath,
		cleanRelPath: cleanRelPath,
	}
	return h, nil
}

func (h *requestHandler) sendError(statusCode int, message string, err error) {
	h.log.Warn().Err(err).Msg(message)
	http.Error(h.w, message, statusCode)
}

func (h *requestHandler) process() {
	meta, found := h.cache.Get(h.r.Context(), h.key)
	if !found {
		h.log.Info().Msg("Cache miss, fetching from upstream")
		h.fetchAndServe(nil)
		return
	}
	defer util.ReturnHeader(meta.Headers)

	h.log.Debug().Msg("Cache hit")

	if meta.StatusCode == http.StatusNotFound {
		h.handleNegativeCache(meta)
		return
	}

	if meta.IsStale(time.Now()) {
		h.log.Info().Msg("Revalidating stale item")
		h.fetchAndServe(meta)
		return
	}

	h.serveFromCache(meta)
}

func (h *requestHandler) serveFromCache(meta *cache.ItemMeta) {
	if util.ClientHasFreshVersion(h.r) {
		logConditionalRequest(h.r, meta, h.log)
		if util.CheckConditional(h.w, h.r, meta.Headers) {
			h.log.Debug().Msg("Conditional check succeeded, returning 304")
			return
		}
		h.log.Debug().Msg("Conditional check failed, serving full response")
	}

	util.CopyWhitelistedHeaders(h.w.Header(), meta.Headers)
	h.w.Header().Set("X-Cache-Status", cacheStatusHit)

	if h.r.Method == http.MethodHead {
		h.w.WriteHeader(meta.StatusCode)
		return
	}

	content, err := h.cache.GetContent(h.r.Context(), h.key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			h.log.Warn().Msg("Metadata found, but content is missing. Refetching.")
			h.cache.Delete(h.r.Context(), h.key)
			h.fetchAndServe(nil)
			return
		}
		h.sendError(http.StatusInternalServerError, "Cache content unavailable", err)
		return
	}
	defer content.Close()

	h.w.WriteHeader(meta.StatusCode)

	buf := util.GetBuffer()
	defer util.ReturnBuffer(buf)
	if _, copyErr := io.CopyBuffer(h.w, content, buf); copyErr != nil {
		h.log.Warn().Err(copyErr).Msg("Error copying cached content to response")
	}
}

func (h *requestHandler) handleNegativeCache(meta *cache.ItemMeta) {
	if meta.IsStale(time.Now()) {
		h.log.Info().Msg("Stale negative cache entry, re-fetching")
		h.cache.Delete(h.r.Context(), h.key)
		h.fetchAndServe(nil)
		return
	}

	h.log.Debug().Msg("Serving 404 from negative cache")
	util.CopyWhitelistedHeaders(h.w.Header(), meta.Headers)
	http.Error(h.w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

func (h *requestHandler) fetchAndServe(revalMeta *cache.ItemMeta) {
	opts := fetch.NewFetchOptions(h.r, revalMeta)
	resInterface, _, shared := h.fetcher.Fetch(h.r.Context(), h.key, h.upstreamURL, opts)

	sharedFetch, ok := resInterface.(*fetch.SharedFetch)
	if !ok {
		if resInterface == nil {
			h.log.Warn().Msg("Fetch initiation failed, possibly due to client disconnect.")
			return
		}
		h.sendError(http.StatusInternalServerError, "Internal server error", fmt.Errorf("unexpected type from singleflight: %T", resInterface))
		return
	}

	h.handleFetchResult(sharedFetch, shared, revalMeta)
}

func (h *requestHandler) handleFetchResult(sharedFetch *fetch.SharedFetch, shared bool, revalMeta *cache.ItemMeta) {
	if shared {
		h.log.Debug().Msg("Shared fetch completed, serving from cache.")
		meta, found := h.cache.Get(h.r.Context(), h.key)
		if !found {
			h.sendError(http.StatusInternalServerError, "Failed to retrieve file after fetch", errors.New("item not found in cache after shared fetch"))
			return
		}
		defer util.ReturnHeader(meta.Headers)
		h.serveFromCache(meta)
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
		h.handleFetchError(err, fetchRes, revalMeta)
		return
	}

	h.handleFetchSuccess(fetchRes)
}

func (h *requestHandler) handleFetchError(err error, fetchRes *fetch.Result, revalMeta *cache.ItemMeta) {
	if revalMeta != nil && (errors.Is(err, fetch.ErrNetwork) || errors.Is(err, fetch.ErrUpstreamServer)) {
		h.handleGracefulOffline(err, revalMeta)
		return
	}

	if errors.Is(err, fetch.ErrUpstreamNotFound) {
		h.handleNegativeCaching(fetchRes)
		return
	}

	if errors.Is(err, fetch.ErrUpstreamNotModified) {
		h.handleRevalidation(fetchRes, revalMeta)
		return
	}

	h.sendError(http.StatusBadGateway, "Upstream fetch failed", err)
}

func (h *requestHandler) handleGracefulOffline(err error, revalMeta *cache.ItemMeta) {
	h.log.Warn().Err(err).Msg("Upstream fetch failed, serving stale content from cache (grace mode).")
	h.serveFromCache(revalMeta)
}

func (h *requestHandler) handleNegativeCaching(fetchRes *fetch.Result) {
	h.log.Info().Msg("Upstream returned 404 Not Found")
	if h.cfg.Cache.NegativeTTL > 0 {
		now := time.Now()
		meta := &cache.ItemMeta{
			Key:         h.key,
			UpstreamURL: h.upstreamURL,
			FetchedAt:   now,
			LastUsedAt:  now,
			StatusCode:  http.StatusNotFound,
			Headers:     util.CopyHeader(fetchRes.Header),
			Size:        0,
			ExpiresAt:   now.Add(h.cfg.Cache.NegativeTTL),
		}
		if putErr := h.cache.Put(h.r.Context(), meta); putErr != nil {
			h.log.Error().Err(putErr).Msg("Failed to save negative cache entry")
		}
	}
	util.CopyWhitelistedHeaders(h.w.Header(), fetchRes.Header)
	http.Error(h.w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

func (h *requestHandler) handleRevalidation(fetchRes *fetch.Result, revalMeta *cache.ItemMeta) {
	if revalMeta == nil {
		h.log.Info().Msg("Conditional cache miss: upstream confirmed not modified. Forwarding 304 to client.")
		util.CopyWhitelistedHeaders(h.w.Header(), fetchRes.Header)
		h.w.WriteHeader(http.StatusNotModified)
		return
	}

	h.log.Info().Msg("Revalidation successful (304), updating metadata.")
	updatedMeta := *revalMeta
	updatedMeta.Headers = util.CopyHeader(revalMeta.Headers)

	// 304 почти всегда без Cache-Control; fallback на старые хедеры
	freshHeaders := fetchRes.Header
	if len(freshHeaders) == 0 {
		freshHeaders = revalMeta.Headers
	}
	updatedMeta.ExpiresAt = cache.CalculateFreshness(
		freshHeaders, time.Now(), h.cleanRelPath, h.cfg.Cache.Overrides,
	)
	updatedMeta.LastUsedAt = time.Now()
	util.UpdateCacheHeaders(updatedMeta.Headers, fetchRes.Header)

	if putErr := h.cache.Put(h.r.Context(), &updatedMeta); putErr != nil {
		h.log.Error().Err(putErr).Msg("Failed to update metadata after 304")
		util.ReturnHeader(updatedMeta.Headers)
		return
	}
	h.serveFromCache(&updatedMeta)
}

func (h *requestHandler) handleFetchSuccess(fetchRes *fetch.Result) {
	meta := h.createCacheMeta(fetchRes)

	util.CopyWhitelistedHeaders(h.w.Header(), meta.Headers)
	h.w.Header().Set("X-Cache-Status", cacheStatusMiss)

	if h.r.Method == http.MethodHead {
		h.w.WriteHeader(meta.StatusCode)
		if err := h.cache.Put(h.r.Context(), meta); err != nil {
			h.log.Error().Err(err).Msg("Failed to save metadata for HEAD request")
		}
		return
	}

	h.streamToClientAndCache(fetchRes, meta)
}

func (h *requestHandler) createCacheMeta(fetchRes *fetch.Result) *cache.ItemMeta {
	now := time.Now()
	headersCopy := util.CopyHeader(fetchRes.Header)
	return &cache.ItemMeta{
		Key:         h.key,
		UpstreamURL: h.upstreamURL,
		FetchedAt:   now,
		LastUsedAt:  now,
		StatusCode:  fetchRes.Status,
		Headers:     headersCopy,
		Size:        fetchRes.Size,
		ExpiresAt:   cache.CalculateFreshness(headersCopy, now, h.cleanRelPath, h.cfg.Cache.Overrides),
	}
}

func (h *requestHandler) streamToClientAndCache(fetchRes *fetch.Result, meta *cache.ItemMeta) {
	pr, pw := io.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)

	ctx := h.r.Context()

	go func(m *cache.ItemMeta) {
		defer wg.Done()
		written, err := h.cache.PutContent(ctx, h.key, pr)
		if err != nil {
			if !util.IsClientDisconnectedError(err) {
				h.log.Error().Err(err).Msg("Failed to write content to cache")
			}
			h.cache.Delete(context.Background(), h.key)
			return
		}
		m.Size = written
		if err := h.cache.Put(ctx, m); err != nil {
			h.log.Error().Err(err).Msg("Failed to save metadata after content write")
		}
	}(meta)

	tee := io.TeeReader(fetchRes.Body, pw)

	h.w.WriteHeader(meta.StatusCode)
	_, copyErr := io.Copy(h.w, tee)

	if copyErr != nil {
		pw.CloseWithError(copyErr)
	} else {
		pw.Close()
	}

	wg.Wait()
}

func (app *Application) handleServeRepoContent(w http.ResponseWriter, r *http.Request) {
	handler, err := app.newRequestHandler(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	handler.process()
}

func (app *Application) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	var sb strings.Builder
	sb.WriteString("OK\n")
	sb.WriteString("Cache status endpoint is active.\n")
	w.Write([]byte(sb.String()))
}

func logConditionalRequest(r *http.Request, meta *cache.ItemMeta, log *logging.Logger) {
	event := log.Debug()
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		event.Str("client_if_none_match", inm)
	}
	if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		event.Str("client_if_modified_since", ims)
	}
	if etag := meta.Headers.Get("ETag"); etag != "" {
		event.Str("cached_etag", etag)
	}
	if lm := meta.Headers.Get("Last-Modified"); lm != "" {
		event.Str("cached_last_modified", lm)
	}
	event.Msg("Checking conditional request")
}
