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

	if r.URL.RawQuery != "" || r.URL.Fragment != "" {
		h.log.Warn().
			Str("path", r.URL.Path).
			Str("query", r.URL.RawQuery).
			Str("fragment", r.URL.Fragment).
			Msg("Request with query parameters or fragment blocked")
		http.Error(w, "Query parameters or fragments are not allowed", http.StatusBadRequest)
		return
	}

	if strings.Contains(relPath, "..") {
		cleanedPath := filepath.Clean(relPath)
		if strings.HasPrefix(cleanedPath, "..") || cleanedPath == ".." {
			h.log.Warn().Str("path", relPath).Str("cleaned_path", cleanedPath).Msg("Path traversal attempt detected and blocked")
			http.Error(w, "Invalid path (traversal attempt)", http.StatusBadRequest)
			return
		}
		h.log.Debug().Str("path", relPath).Str("cleaned_path", cleanedPath).Msg("Path contained '..' but was cleaned")
		relPath = cleanedPath
	}
	if relPath == "." {
		relPath = ""
	}

	cacheKey, err := util.GenerateCacheKey(h.repoCfg.Name, relPath)
	if err != nil {
		h.log.Error().Err(err).Str("repo", h.repoCfg.Name).Str("relPath", relPath).Msg("Failed to generate cache key")
		http.Error(w, "Internal server error (cache key generation)", http.StatusInternalServerError)
		return
	}

	upstreamURL := h.repoCfg.URL + relPath

	h.log.Debug().
		Str("method", r.Method).
		Str("relPath", relPath).
		Str("cacheKey", cacheKey).
		Str("upstreamURL", upstreamURL).
		Msg("Handling repository request")

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
	h.handleCacheMiss(w, r, cacheKey, upstreamURL)
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
			h.handleCacheMiss(w, r, key, upstreamURL)
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
		h.revalidateAndServe(w, r, key, upstreamURL, meta)
	} else {
		w.Header().Set("X-Cache-Status", "HIT")
		h.serveFromCache(w, r, key, relPath, meta)
	}
}

func (h *RepoHandler) revalidateAndServe(w http.ResponseWriter, r *http.Request,
	key, upstreamURL string, currentMeta *cache.ItemMeta) {

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
		h.log.Info().Str("key", key).Int("status", fetchRes.Status).Msg("Revalidation successful: new content fetched.")
		defer fetchRes.Body.Close()
		h.storeAndServe(w, r, key, upstreamURL, fetchRes, "UPDATED")
		return
	}

	derivedRelPath := strings.TrimPrefix(upstreamURL, h.repoCfg.URL)

	if currentMeta.UpstreamURL != "" {
		derivedRelPath = strings.TrimPrefix(currentMeta.UpstreamURL, h.repoCfg.URL)
	}

	if errors.Is(fetchErr, fetch.ErrUpstreamNotModified) {
		h.log.Info().Str("key", key).Msg("Revalidation successful: item not modified (304).")
		if err := h.cm.UpdateValidatedAt(r.Context(), key, time.Now()); err != nil {
			h.log.Warn().Err(err).Str("key", key).Msg("Failed to update validation timestamp in cache.")
		}
		w.Header().Set("X-Cache-Status", "VALIDATED")
		h.serveFromCache(w, r, key, derivedRelPath, currentMeta)
		return
	}

	h.log.Warn().Err(fetchErr).Str("key", key).Msg("Revalidation fetch failed. Serving stale content if possible.")
	w.Header().Set("X-Cache-Status", "STALE_REVAL_FAILED")
	h.serveFromCache(w, r, key, derivedRelPath, currentMeta)
}

func (h *RepoHandler) handleCacheMiss(w http.ResponseWriter, r *http.Request,
	key, upstreamURL string) {
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

	h.storeAndServe(w, r, key, upstreamURL, fetchRes, "MISS_FETCHED")
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
				emptyHeaders := make(http.Header)
				popts := cache.PutOptions{
					UpstreamURL: upstreamURL,
					StatusCode:  http.StatusNotFound,
					Headers:     emptyHeaders,
					FetchedAt:   time.Now(),
					Size:        0,
				}
				if _, putErr := h.cm.Put(context.Background(), key, nil, popts); putErr != nil {
					h.log.Warn().Err(putErr).Str("key", key).Msg("Failed to store negative cache entry for 404.")
				} else {
					h.log.Debug().Str("key", key).Msg("Negative cache entry for 404 stored.")
				}
			}()
		}
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	h.log.Error().Err(err).Str("key", key).Str("upstreamURL", upstreamURL).Msg("Failed to fetch item from upstream on MISS.")
	status := http.StatusBadGateway
	if errors.Is(err, fetch.ErrUpstreamServerErr) {
		status = http.StatusBadGateway
	} else if errors.Is(err, fetch.ErrUpstreamClientErr) {
		status = http.StatusBadGateway
	} else if errors.Is(err, fetch.ErrNetwork) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		status = http.StatusGatewayTimeout
	}
	http.Error(w, http.StatusText(status), status)
}

func (h *RepoHandler) storeAndServe(w http.ResponseWriter, r *http.Request,
	key, fetchedUpstreamURL string, fetchRes *fetch.Result, xCacheStat string) {

	popts := cache.PutOptions{
		UpstreamURL: fetchedUpstreamURL,
		StatusCode:  fetchRes.Status,
		Headers:     fetchRes.Header,
		Size:        fetchRes.Size,
		FetchedAt:   time.Now(),
	}

	if canonicalURL := fetchRes.Header.Get("X-Upstream-Url"); canonicalURL != "" {
		popts.UpstreamURL = canonicalURL
	} else if contentLoc := fetchRes.Header.Get("Content-Location"); contentLoc != "" {
		popts.UpstreamURL = contentLoc
	}

	pr, pw := io.Pipe()
	var storeWg sync.WaitGroup
	storeWg.Add(1)

	go func() {
		defer storeWg.Done()
		_, putErr := h.cm.Put(context.Background(), key, pr, popts)
		if putErr != nil {
			if !errors.Is(putErr, io.ErrClosedPipe) {
				h.log.Error().Err(putErr).Str("key", key).Msg("Error storing item to cache during Tee.")
			}
			_ = pr.CloseWithError(putErr)
		}
	}()

	teeBody := io.TeeReader(fetchRes.Body, pw)

	if h.checkClientConditional(w, r, fetchRes.ModTime, fetchRes.Header.Get("ETag")) {
		w.Header().Set("X-Cache-Status", xCacheStat+"_CLIENT_NOT_MODIFIED")
		_, copyErr := io.Copy(io.Discard, teeBody)
		if copyErr != nil && !util.IsClientDisconnectedError(copyErr) {
			h.log.Warn().Err(copyErr).Str("key", key).Msg("Error discarding body for 304 response after tee.")
		}
		if copyErr != nil {
			_ = pw.CloseWithError(copyErr)
		} else {
			_ = pw.Close()
		}
		storeWg.Wait()
		return
	}

	w.Header().Set("X-Cache-Status", xCacheStat)
	h.setResponseHeaders(w, fetchRes.Header, fetchRes.ModTime)
	util.SelectProxyHeaders(w.Header(), fetchRes.Header)

	if r.Method == http.MethodHead {
		if fetchRes.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(fetchRes.Size, 10))
		}
		w.WriteHeader(fetchRes.Status)
		_, _ = io.Copy(io.Discard, teeBody)
		_ = pw.Close()
	} else {
		if fetchRes.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(fetchRes.Size, 10))
		}
		w.WriteHeader(fetchRes.Status)

		_, err := io.Copy(w, teeBody)
		if err != nil {
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

		h.handleCacheMiss(w, r, key, meta.UpstreamURL)
		return
	}
	defer cacheReadRes.Content.Close()

	h.setResponseHeaders(w, meta.Headers, itemModTime)
	util.SelectProxyHeaders(w.Header(), meta.Headers)

	if r.Method == http.MethodHead {
		if meta.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
		}
		w.WriteHeader(meta.StatusCode)
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
		w.WriteHeader(meta.StatusCode)
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

	if imsHdr := r.Header.Get("If-Modified-Since"); imsHdr != "" {
		if t, err := http.ParseTime(imsHdr); err == nil {
			if !resModTime.IsZero() && !resModTime.After(t) {
				w.WriteHeader(http.StatusNotModified)
				return true
			}
		}
	}
	return false
}

func (h *RepoHandler) setResponseHeaders(w http.ResponseWriter, itemHeaders http.Header, itemModTime time.Time) {
	if ct := itemHeaders.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	} else {
		h.log.Debug().Msg("Content-Type not provided by upstream/cache.")
	}

	if !itemModTime.IsZero() {
		w.Header().Set("Last-Modified", itemModTime.UTC().Format(http.TimeFormat))
	}

	if etag := itemHeaders.Get("ETag"); etag != "" {
		w.Header().Set("ETag", etag)
	}

	if ar := itemHeaders.Get("Accept-Ranges"); ar != "" {
		w.Header().Set("Accept-Ranges", ar)
	}
}
