package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/fetch"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type RepoHandler struct {
	repoCfg  config.Repository
	srvCfg   config.ServerConfig
	cacheCfg config.CacheConfig
	cm       cache.Manager
	fetcher  *fetch.Coordinator
	log      zerolog.Logger
}

func newRepoHandler(rcfg config.Repository, scfg config.ServerConfig, ccfg config.CacheConfig,
	cm cache.Manager, fc *fetch.Coordinator, parentLog zerolog.Logger) *RepoHandler {
	return &RepoHandler{
		repoCfg: rcfg, srvCfg: scfg, cacheCfg: ccfg, cm: cm, fetcher: fc,
		log: parentLog.With().Str("repo", rcfg.Name).Logger(),
	}
}

func (h *RepoHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	relPath := strings.TrimPrefix(r.URL.Path, "/")
	if strings.Contains(relPath, "..") {
		h.log.Warn().Str("path", relPath).Msg("Path traversal attempt detected")
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	cacheKey, err := util.GenerateCacheKey(h.repoCfg.Name, relPath)
	if err != nil {
		h.log.Error().Err(err).Str("repo", h.repoCfg.Name).Str("path", relPath).Msg("Failed to generate cache key")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	upstreamURL := h.repoCfg.URL + relPath

	h.log.Debug().Str("meth", r.Method).Str("path", relPath).Str("key", cacheKey).Str("url", upstreamURL).Msg("Handling request")

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	cacheRes, err := h.cm.Get(r.Context(), cacheKey)
	if err != nil && !errors.Is(err, cache.ErrNotFound) {
		h.log.Error().Err(err).Str("key", cacheKey).Msg("Cache get error")
		http.Error(w, "Cache error", http.StatusInternalServerError)
		return
	}

	if cacheRes != nil && cacheRes.Hit {
		h.log.Info().Str("key", cacheKey).Str("status", "HIT").Msg("Cache hit")
		h.handleCacheHit(w, r, cacheKey, upstreamURL, relPath, cacheRes)
		return
	}

	h.log.Info().Str("key", cacheKey).Str("status", "MISS").Msg("Cache miss")
	h.handleCacheMiss(w, r, cacheKey, upstreamURL, relPath)
}

func (h *RepoHandler) handleCacheHit(w http.ResponseWriter, r *http.Request,
	key, upstreamURL, relPath string, cacheRes *cache.GetResult) {

	meta := cacheRes.Meta
	if cacheRes.Content != nil {
		defer cacheRes.Content.Close()
	}

	if meta.StatusCode == http.StatusNotFound {
		h.log.Debug().Str("key", key).Msg("Negative cache hit (404)")

		if h.cacheCfg.NegativeTTL.StdDuration() > 0 && time.Since(meta.FetchedAt) > h.cacheCfg.NegativeTTL.StdDuration() {
			h.log.Info().Str("key", key).Msg("Expired negative cache entry, re-fetching.")
			go h.cm.Delete(context.Background(), key)
			h.handleCacheMiss(w, r, key, upstreamURL, relPath)
			return
		}
		w.Header().Set("X-Cache-Status", "HIT_NEGATIVE")
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	needsReval := false
	now := time.Now()
	if meta.IsStale(now) {
		h.log.Debug().Str("key", key).Time("expires_at", meta.ExpiresAt).Msg("Item stale based on ExpiresAt/Cache-Control.")
		needsReval = true
	}

	if !needsReval && h.cacheCfg.RevalidateHitTTL.StdDuration() > 0 {
		if meta.ValidatedAt.IsZero() || now.Sub(meta.ValidatedAt) > h.cacheCfg.RevalidateHitTTL.StdDuration() {
			h.log.Debug().Str("key", key).Time("validated_at", meta.ValidatedAt).Msg("Item requires revalidation due to RevalidateOnHitTTL.")
			needsReval = true
		}
	}

	if clientCC := util.ParseCacheControl(r.Header.Get("Cache-Control")); clientCC["no-cache"] != "" {
		h.log.Debug().Str("key", key).Msg("Client requested no-cache, forcing revalidation.")
		needsReval = true
	}

	if needsReval {
		h.revalidateAndServe(w, r, key, upstreamURL, relPath, meta)
	} else {
		w.Header().Set("X-Cache-Status", "HIT")
		h.serveFromCache(w, r, key, relPath, meta)
	}
}

func (h *RepoHandler) revalidateAndServe(w http.ResponseWriter, r *http.Request,
	key, upstreamURL, relPath string, currentMeta *cache.ItemMeta) {

	h.log.Debug().Str("key", key).Msg("Revalidating with upstream.")
	w.Header().Set("X-Cache-Status", "REVALIDATING")

	fetchOpts := &fetch.Options{}
	if lmStr := currentMeta.Headers.Get("Last-Modified"); lmStr != "" {
		if t, err := http.ParseTime(lmStr); err == nil {
			fetchOpts.IfModSince = t
		}
	} else if !currentMeta.FetchedAt.IsZero() && currentMeta.StatusCode == http.StatusOK {
		fetchOpts.IfModSince = currentMeta.FetchedAt
	}
	if etag := currentMeta.Headers.Get("ETag"); etag != "" {
		fetchOpts.IfNoneMatch = etag
	}

	revalKey := "revalidate:" + key
	fetchRes, fetchErr := h.fetcher.Fetch(r.Context(), revalKey, upstreamURL, fetchOpts)

	if fetchErr == nil {
		h.log.Info().Str("key", key).Msg("Revalidation successful: new content fetched.")
		defer fetchRes.Body.Close()
		h.storeAndServe(w, r, key, relPath, fetchRes, "UPDATED")
		return
	}

	if errors.Is(fetchErr, fetch.ErrUpstreamNotModified) {
		h.log.Info().Str("key", key).Msg("Revalidation successful: item not modified (304).")
		if err := h.cm.UpdateValidatedAt(r.Context(), key, time.Now()); err != nil {
			h.log.Warn().Err(err).Str("key", key).Msg("Failed to update validation timestamp in cache.")
		}
		w.Header().Set("X-Cache-Status", "VALIDATED")
		h.serveFromCache(w, r, key, relPath, currentMeta)
		return
	}

	h.log.Warn().Err(fetchErr).Str("key", key).Msg("Revalidation fetch failed. Serving stale content.")
	w.Header().Set("X-Cache-Status", "STALE_REVAL_FAILED")
	h.serveFromCache(w, r, key, relPath, currentMeta)
}

func (h *RepoHandler) handleCacheMiss(w http.ResponseWriter, r *http.Request,
	key, upstreamURL, relPath string) {
	w.Header().Set("X-Cache-Status", "MISS")

	fetchOpts := &fetch.Options{}
	if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, err := http.ParseTime(ims); err == nil {
			fetchOpts.IfModSince = t
		}
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		fetchOpts.IfNoneMatch = inm
	}

	fetchRes, fetchErr := h.fetcher.Fetch(r.Context(), key, upstreamURL, fetchOpts)
	if fetchErr != nil {
		h.handleFetchError(w, r, key, upstreamURL, fetchErr)
		return
	}
	defer fetchRes.Body.Close()

	h.storeAndServe(w, r, key, relPath, fetchRes, "MISS_FETCHED")
}

func (h *RepoHandler) handleFetchError(w http.ResponseWriter, _ *http.Request, key, upstreamURL string, err error) {
	if errors.Is(err, fetch.ErrUpstreamNotModified) {
		h.log.Info().Str("key", key).Msg("Upstream returned 304 for MISS (client has current).")
		w.Header().Set("X-Cache-Status", "MISS_UPSTREAM_NOT_MODIFIED")
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if errors.Is(err, fetch.ErrUpstreamNotFound) {
		h.log.Info().Str("key", key).Msg("Upstream returned 404 for MISS.")
		w.Header().Set("X-Cache-Status", "MISS_UPSTREAM_NOT_FOUND")
		if h.cacheCfg.NegativeTTL.StdDuration() > 0 {
			go func() {
				popts := cache.PutOptions{
					UpstreamURL: upstreamURL, StatusCode: http.StatusNotFound,
					Headers: make(http.Header), FetchedAt: time.Now(),
				}
				if _, putErr := h.cm.Put(context.Background(), key, nil, popts); putErr != nil {
					h.log.Warn().Err(putErr).Str("key", key).Msg("Failed to store negative cache entry for 404.")
				}
			}()
		}
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	h.log.Error().Err(err).Str("key", key).Msg("Failed to fetch item from upstream on MISS.")
	status := http.StatusBadGateway
	if errors.Is(err, fetch.ErrUpstreamServerErr) {
		status = http.StatusBadGateway
	}
	if errors.Is(err, fetch.ErrUpstreamClientErr) {
		status = http.StatusBadGateway
	}
	if errors.Is(err, fetch.ErrNetwork) || errors.Is(err, context.DeadlineExceeded) {
		status = http.StatusGatewayTimeout
	}
	http.Error(w, http.StatusText(status), status)
}

func (h *RepoHandler) storeAndServe(w http.ResponseWriter, r *http.Request,
	key, relPath string, fetchRes *fetch.Result, xCacheStat string) {

	popts := cache.PutOptions{
		UpstreamURL: fetchRes.Header.Get("X-Upstream-Url"),
		StatusCode:  fetchRes.Status,
		Headers:     fetchRes.Header,
		Size:        fetchRes.Size,
		FetchedAt:   time.Now(),
	}
	if popts.UpstreamURL == "" {
		popts.UpstreamURL = fetchRes.Header.Get("Content-Location")
	}

	pr, pw := io.Pipe()
	var storeWg sync.WaitGroup
	storeWg.Add(1)

	go func() {
		defer storeWg.Done()
		_, putErr := h.cm.Put(context.Background(), key, pr, popts)
		if putErr != nil {
			h.log.Error().Err(putErr).Str("key", key).Msg("Error storing item to cache during Tee.")
			_ = pr.CloseWithError(putErr)
		}
	}()

	teeBody := io.TeeReader(fetchRes.Body, pw)

	if h.checkClientConditional(w, r, fetchRes.ModTime, fetchRes.Header.Get("ETag")) {
		w.Header().Set("X-Cache-Status", xCacheStat+"_CLIENT_NOT_MODIFIED")
		_, _ = io.Copy(io.Discard, teeBody)
		_ = pw.Close()
		storeWg.Wait()
		return
	}

	w.Header().Set("X-Cache-Status", xCacheStat)
	h.setResponseHeaders(w, relPath, fetchRes.Header, fetchRes.ModTime)
	util.SelectProxyHeaders(w.Header(), fetchRes.Header)

	if r.Method == http.MethodHead {
		if fetchRes.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(fetchRes.Size, 10))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(io.Discard, teeBody)
	} else {
		if fetchRes.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(fetchRes.Size, 10))
		}
		w.WriteHeader(http.StatusOK)

		if _, err := io.Copy(w, teeBody); err != nil {
			if !util.IsClientDisconnectedError(err) {
				h.log.Error().Err(err).Str("key", key).Msg("Error streaming response to client.")
			}
			_ = pw.CloseWithError(err)
		} else {
			_ = pw.Close()
		}
	}
	storeWg.Wait()
}

func (h *RepoHandler) serveFromCache(w http.ResponseWriter, r *http.Request,
	key, relPath string, meta *cache.ItemMeta) {

	var itemModTime time.Time
	if lmStr := meta.Headers.Get("Last-Modified"); lmStr != "" {
		if t, err := http.ParseTime(lmStr); err == nil {
			itemModTime = t
		}
	}

	if h.checkClientConditional(w, r, itemModTime, meta.Headers.Get("ETag")) {
		return
	}

	cacheReadRes, err := h.cm.Get(r.Context(), key)
	if err != nil || !cacheReadRes.Hit || cacheReadRes.Content == nil {
		h.log.Error().Err(err).Str("key", key).Msg("Failed to re-open cache item for serving, or item vanished.")
		h.handleCacheMiss(w, r, key, meta.UpstreamURL, relPath)
		return
	}
	defer cacheReadRes.Content.Close()

	h.setResponseHeaders(w, relPath, meta.Headers, itemModTime)
	util.SelectProxyHeaders(w.Header(), meta.Headers)

	if r.Method == http.MethodHead {
		if meta.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	contentSeeker, isSeeker := cacheReadRes.Content.(io.ReadSeeker)
	if isSeeker {
		serveName := filepath.Base(relPath)
		if serveName == "." || serveName == "/" {
			serveName = ""
		}
		http.ServeContent(w, r, serveName, itemModTime, contentSeeker)
	} else {
		if meta.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
		}
		w.WriteHeader(http.StatusOK)
		if _, err := io.Copy(w, cacheReadRes.Content); err != nil {
			if !util.IsClientDisconnectedError(err) {
				h.log.Error().Err(err).Str("key", key).Msg("Error streaming non-seekable cache content to client.")
			}
		}
	}
}

func (h *RepoHandler) checkClientConditional(w http.ResponseWriter, r *http.Request, resModTime time.Time, resETag string) bool {
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if util.CompareETags(inm, resETag) {
			w.WriteHeader(http.StatusNotModified)
			return true
		}
		return false
	}

	if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, err := http.ParseTime(ims); err == nil {
			if !resModTime.IsZero() && !resModTime.After(t) {
				w.WriteHeader(http.StatusNotModified)
				return true
			}
		}
	}
	return false
}

func (h *RepoHandler) setResponseHeaders(w http.ResponseWriter, relPath string, itemHeaders http.Header, itemModTime time.Time) {
	if ct := itemHeaders.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	} else {
		w.Header().Set("Content-Type", util.GetContentType(relPath))
	}

	if !itemModTime.IsZero() {
		w.Header().Set("Last-Modified", itemModTime.UTC().Format(http.TimeFormat))
	}

	if etag := itemHeaders.Get("ETag"); etag != "" {
		w.Header().Set("ETag", etag)
	}
}
