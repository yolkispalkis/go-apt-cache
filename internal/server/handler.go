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

func newRequestHandler(w http.ResponseWriter, r *http.Request) (*requestHandler, error) {
	repo := r.Context().Value(repoContextKey).(config.Repository)
	relPath := chi.URLParam(r, "*")
	cleanRelPath := filepath.Clean(relPath)

	if strings.HasPrefix(cleanRelPath, "..") || cleanRelPath == ".." || cleanRelPath == "." {
		logger := r.Context().Value(loggerContextKey).(*logging.Logger)
		logger.Warn().
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
		log:          r.Context().Value(loggerContextKey).(*logging.Logger).WithContext("key", key, "repo", repo.Name),
		cache:        r.Context().Value(cacheContextKey).(cache.Manager),
		fetcher:      r.Context().Value(fetcherContextKey).(*fetch.Coordinator),
		cfg:          r.Context().Value(configContextKey).(*config.Config),
		repo:         repo,
		key:          key,
		upstreamURL:  repo.URL + cleanRelPath,
		cleanRelPath: cleanRelPath,
	}
	return h, nil
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
	logConditionalRequest(h.r, meta, h.log)

	if clientHasFreshVersion(h.r) {
		if util.CheckConditional(h.w, h.r, meta.Headers) {
			h.log.Debug().Msg("Conditional check succeeded, returning 304")
			return
		}
	}

	h.log.Debug().Msg("Conditional check failed or not applicable, serving full response")

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
		h.log.Error().Err(err).Msg("Failed to get content for cached metadata")
		http.Error(h.w, "Cache content unavailable", http.StatusInternalServerError)
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
	opts := extractOptions(h.r, revalMeta)
	resInterface, _, shared := h.fetcher.Fetch(h.r.Context(), h.key, h.upstreamURL, opts)

	sharedFetch, ok := resInterface.(*fetch.SharedFetch)
	if !ok {
		if resInterface == nil {
			h.log.Warn().Msg("Fetch initiation failed, possibly due to client disconnect.")
			return
		}
		h.log.Error().Msgf("Unexpected type from singleflight: %T", resInterface)
		http.Error(h.w, "Internal server error", http.StatusInternalServerError)
		return
	}

	h.handleFetchResult(sharedFetch, shared, revalMeta)
}

func (h *requestHandler) handleFetchResult(sharedFetch *fetch.SharedFetch, shared bool, revalMeta *cache.ItemMeta) {
	if shared {
		h.log.Debug().Msg("Shared fetch completed, serving from cache.")
		meta, found := h.cache.Get(h.r.Context(), h.key)
		if !found {
			h.log.Error().Msg("Item not found in cache after shared fetch")
			http.Error(h.w, "Failed to retrieve file after fetch", http.StatusInternalServerError)
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

	h.log.Warn().Err(err).Msg("Failed to fetch from upstream")
	http.Error(h.w, "Upstream fetch failed", http.StatusBadGateway)
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
			UpstreamURL: fetchRes.Header.Get("Request-Url"),
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
	updatedMeta.ExpiresAt = calculateFreshness(fetchRes.Header, time.Now(), h.cleanRelPath, h.cfg.Cache.Overrides)
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
		ExpiresAt:   calculateFreshness(headersCopy, now, h.cleanRelPath, h.cfg.Cache.Overrides),
	}
}

func (h *requestHandler) streamToClientAndCache(fetchRes *fetch.Result, meta *cache.ItemMeta) {
	pr, pw := io.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)

	go func(m *cache.ItemMeta) {
		defer wg.Done()
		written, err := h.cache.PutContent(h.r.Context(), h.key, pr)
		if err != nil {
			if !util.IsClientDisconnectedError(err) {
				h.log.Error().Err(err).Msg("Failed to write content to cache")
			}
			h.cache.Delete(context.Background(), h.key)
			return
		}
		m.Size = written
		if err := h.cache.Put(context.Background(), m); err != nil {
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

func handleServeRepoContent(w http.ResponseWriter, r *http.Request) {
	handler, err := newRequestHandler(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	handler.process()
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
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

func clientHasFreshVersion(r *http.Request) bool {
	return r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != ""
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
