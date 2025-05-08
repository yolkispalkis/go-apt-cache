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

type RepositoryHandler struct {
	repoConfig   config.Repository
	serverConfig config.ServerConfig
	cacheConfig  config.CacheConfig
	cacheManager cache.CacheManager
	fetcher      *fetch.Coordinator
	logger       zerolog.Logger
}

func NewRepositoryHandler(
	repoCfg config.Repository,
	serverCfg config.ServerConfig,
	cacheCfg config.CacheConfig,
	cm cache.CacheManager,
	fc *fetch.Coordinator,
	parentLogger zerolog.Logger,
) *RepositoryHandler {
	return &RepositoryHandler{
		repoConfig:   repoCfg,
		serverConfig: serverCfg,
		cacheConfig:  cacheCfg,
		cacheManager: cm,
		fetcher:      fc,
		logger:       parentLogger.With().Str("repository", repoCfg.Name).Logger(),
	}
}

func (h *RepositoryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	relativePath := strings.TrimPrefix(r.URL.Path, "/")
	if strings.Contains(relativePath, "..") {
		h.logger.Warn().Str("path", relativePath).Msg("Path traversal attempt detected")
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	cacheKey, err := util.GenerateCacheKey(h.repoConfig.Name, relativePath)
	if err != nil {
		h.logger.Error().Err(err).Str("repo", h.repoConfig.Name).Str("relative_path", relativePath).Msg("Failed to generate cache key")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	upstreamURL := h.repoConfig.URL + relativePath

	h.logger.Debug().Str("method", r.Method).Str("path", relativePath).Str("cache_key", cacheKey).Str("upstream_url", upstreamURL).Msg("Handling request")

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		h.logger.Warn().Str("method", r.Method).Msg("Method not allowed")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	cacheResult, err := h.cacheManager.Get(r.Context(), cacheKey)
	if err != nil && !errors.Is(err, cache.ErrNotFound) {
		h.logger.Error().Err(err).Str("key", cacheKey).Msg("Error looking up item in cache")
		http.Error(w, "Cache error", http.StatusInternalServerError)
		return
	}

	if cacheResult != nil && cacheResult.Hit {
		h.logger.Info().Str("key", cacheKey).Str("status", "HIT").Msg("Cache hit")
		h.handleCacheHit(w, r, cacheKey, upstreamURL, relativePath, cacheResult)
		return
	}

	h.logger.Info().Str("key", cacheKey).Str("status", "MISS").Msg("Cache miss")
	h.handleCacheMiss(w, r, cacheKey, upstreamURL, relativePath)
}

func (h *RepositoryHandler) handleCacheHit(w http.ResponseWriter, r *http.Request, cacheKey, upstreamURL, relativePath string, cacheResult *cache.CacheResult) {
	meta := cacheResult.Metadata

	if cacheResult.Content != nil {
		defer cacheResult.Content.Close()
	}

	if meta.StatusCode == http.StatusNotFound {
		h.logger.Debug().Str("key", cacheKey).Msg("Negative cache hit (404)")

		if h.cacheConfig.NegativeCacheTTL.Duration() > 0 && time.Since(meta.FetchedAt) > h.cacheConfig.NegativeCacheTTL.Duration() {
			h.logger.Info().Str("key", cacheKey).Msg("Expired negative cache entry, treating as miss.")

			go h.cacheManager.Delete(context.Background(), cacheKey)
			h.handleCacheMiss(w, r, cacheKey, upstreamURL, relativePath)
			return
		}
		w.Header().Set("X-Cache-Status", "HIT_NEGATIVE")
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	needsRevalidation := false
	now := time.Now()

	if meta.IsStale(now) {
		h.logger.Debug().Str("key", cacheKey).Time("expires_at", meta.ExpiresAt).Msg("Item stale based on ExpiresAt/Cache-Control.")
		needsRevalidation = true
	}

	if !needsRevalidation && h.cacheConfig.RevalidateOnHitTTL.Duration() > 0 {
		if meta.ValidatedAt.IsZero() || now.Sub(meta.ValidatedAt) > h.cacheConfig.RevalidateOnHitTTL.Duration() {
			h.logger.Debug().Str("key", cacheKey).Time("validated_at", meta.ValidatedAt).Msg("Item requires revalidation due to RevalidateOnHitTTL.")
			needsRevalidation = true
		}
	}

	if clientCC := util.ParseCacheControl(r.Header.Get("Cache-Control")); clientCC["no-cache"] != "" {
		h.logger.Debug().Str("key", cacheKey).Msg("Client requested no-cache, forcing revalidation.")
		needsRevalidation = true
	}

	if needsRevalidation {
		h.logger.Debug().Str("key", cacheKey).Msg("Revalidating with upstream.")
		w.Header().Set("X-Cache-Status", "REVALIDATING")
		fetchOpts := &fetch.FetchOptions{}
		if !meta.ModTime.IsZero() {
			fetchOpts.IfModifiedSince = meta.ModTime
		}
		if etag := meta.Headers.Get("ETag"); etag != "" {
			fetchOpts.IfNoneMatch = etag
		}

		revalKey := "revalidate:" + cacheKey
		fetchResult, fetchErr := h.fetcher.Fetch(r.Context(), revalKey, upstreamURL, fetchOpts)

		if fetchErr == nil {
			h.logger.Info().Str("key", cacheKey).Msg("Revalidation successful: new content fetched.")
			defer fetchResult.Body.Close()

			h.storeAndServe(w, r, cacheKey, relativePath, fetchResult, "UPDATED")
			return
		}

		if errors.Is(fetchErr, fetch.ErrUpstreamNotModified) {
			h.logger.Info().Str("key", cacheKey).Msg("Revalidation successful: item not modified (304).")

			if err := h.cacheManager.UpdateValidatedAt(r.Context(), cacheKey, time.Now()); err != nil {
				h.logger.Warn().Err(err).Str("key", cacheKey).Msg("Failed to update validation timestamp in cache.")
			}
			w.Header().Set("X-Cache-Status", "VALIDATED")

			h.serveFromCache(w, r, cacheKey, relativePath, meta)
			return
		}

		h.logger.Warn().Err(fetchErr).Str("key", cacheKey).Msg("Revalidation fetch failed. Serving stale content.")
		w.Header().Set("X-Cache-Status", "STALE_REVAL_FAILED")

		h.serveFromCache(w, r, cacheKey, relativePath, meta)
		return
	}

	w.Header().Set("X-Cache-Status", "HIT")
	h.serveFromCache(w, r, cacheKey, relativePath, meta)
}

func (h *RepositoryHandler) handleCacheMiss(w http.ResponseWriter, r *http.Request, cacheKey, upstreamURL, relativePath string) {
	w.Header().Set("X-Cache-Status", "MISS")

	fetchOpts := &fetch.FetchOptions{}
	if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, err := http.ParseTime(ims); err == nil {
			fetchOpts.IfModifiedSince = t
		}
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		fetchOpts.IfNoneMatch = inm
	}

	fetchResult, fetchErr := h.fetcher.Fetch(r.Context(), cacheKey, upstreamURL, fetchOpts)

	if fetchErr != nil {
		if errors.Is(fetchErr, fetch.ErrUpstreamNotModified) {

			h.logger.Info().Str("key", cacheKey).Msg("Upstream returned 304 for MISS (client is current).")
			w.Header().Set("X-Cache-Status", "MISS_UPSTREAM_NOT_MODIFIED")
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if errors.Is(fetchErr, fetch.ErrUpstreamNotFound) {
			h.logger.Info().Str("key", cacheKey).Msg("Upstream returned 404 for MISS.")
			w.Header().Set("X-Cache-Status", "MISS_UPSTREAM_NOT_FOUND")

			if h.cacheConfig.NegativeCacheTTL.Duration() > 0 {
				go func() {
					putOpts := cache.CachePutOptions{
						UpstreamURL: upstreamURL,
						StatusCode:  http.StatusNotFound,
						Headers:     make(http.Header),
						Size:        0,
						FetchedAt:   time.Now(),
					}

					if _, err := h.cacheManager.Put(context.Background(), cacheKey, nil, putOpts); err != nil {
						h.logger.Warn().Err(err).Str("key", cacheKey).Msg("Failed to store negative cache entry for 404.")
					}
				}()
			}
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}

		h.logger.Error().Err(fetchErr).Str("key", cacheKey).Msg("Failed to fetch item from upstream on MISS.")
		status := http.StatusBadGateway
		if errors.Is(fetchErr, fetch.ErrUpstreamServer) {
			status = http.StatusBadGateway
		}
		if errors.Is(fetchErr, fetch.ErrUpstreamOtherClient) {
			status = http.StatusBadGateway
		}
		if errors.Is(fetchErr, fetch.ErrNetwork) || errors.Is(fetchErr, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}

		http.Error(w, http.StatusText(status), status)
		return
	}
	defer fetchResult.Body.Close()

	h.storeAndServe(w, r, cacheKey, relativePath, fetchResult, "MISS_FETCHED")
}

func (h *RepositoryHandler) storeAndServe(w http.ResponseWriter, r *http.Request, cacheKey, relativePath string, fetchResult *fetch.FetchResult, xCacheStatus string) {

	putOpts := cache.CachePutOptions{
		UpstreamURL: fetchResult.Header.Get("X-Upstream-Url"),
		StatusCode:  fetchResult.StatusCode,
		Headers:     fetchResult.Header,
		Size:        fetchResult.Size,
		FetchedAt:   time.Now(),
	}
	if putOpts.UpstreamURL == "" {
		putOpts.UpstreamURL = fetchResult.Header.Get("Content-Location")
	}

	pr, pw := io.Pipe()

	var newCacheMeta *cache.CacheItemMetadata
	var putErr error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer pw.Close()
		newCacheMeta, putErr = h.cacheManager.Put(r.Context(), cacheKey, pr, putOpts)
		if putErr != nil {
			h.logger.Error().Err(putErr).Str("key", cacheKey).Msg("Error storing item to cache during Tee.")

			pr.CloseWithError(putErr)
		}
	}()

	teeReader := io.TeeReader(fetchResult.Body, pw)

	if h.checkClientConditionalHeaders(w, r, fetchResult.ModTime, fetchResult.Header.Get("ETag")) {
		w.Header().Set("X-Cache-Status", xCacheStatus+"_CLIENT_NOT_MODIFIED")

		io.Copy(io.Discard, teeReader)
		pw.Close()
		wg.Wait()

		return
	}

	w.Header().Set("X-Cache-Status", xCacheStatus)
	if contentType := fetchResult.Header.Get("Content-Type"); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	} else {
		w.Header().Set("Content-Type", util.GetContentType(relativePath))
	}
	if !fetchResult.ModTime.IsZero() {
		w.Header().Set("Last-Modified", fetchResult.ModTime.UTC().Format(http.TimeFormat))
	}
	if etag := fetchResult.Header.Get("ETag"); etag != "" {
		w.Header().Set("ETag", etag)
	}

	util.SelectProxyHeaders(w.Header(), fetchResult.Header)

	if r.Method == http.MethodHead {
		if fetchResult.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(fetchResult.Size, 10))
		}
		w.WriteHeader(http.StatusOK)
		io.Copy(io.Discard, teeReader)
		pw.Close()
		wg.Wait()
		return
	}

	if fetchResult.Size >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(fetchResult.Size, 10))
	}
	w.WriteHeader(http.StatusOK)

	writtenBytes, copyErr := io.Copy(w, teeReader)

	if copyErr != nil && !util.IsClientDisconnectedError(copyErr) {
		h.logger.Error().Err(copyErr).Str("key", cacheKey).Msg("Error streaming response to client.")
		pw.CloseWithError(copyErr)
	} else {
		pw.Close()
	}

	wg.Wait()

	if putErr != nil {
		h.logger.Error().Err(putErr).Str("key", cacheKey).Msg("Caching failed after serving client (or during).")

		if newCacheMeta != nil {
			go h.cacheManager.Delete(context.Background(), cacheKey)
		}
	} else {
		h.logger.Debug().Str("key", cacheKey).Int64("bytes_served", writtenBytes).Msg("Successfully served and initiated caching.")
	}
}

func (h *RepositoryHandler) serveFromCache(w http.ResponseWriter, r *http.Request, cacheKey, relativePath string, meta *cache.CacheItemMetadata) {

	if h.checkClientConditionalHeaders(w, r, meta.ModTime, meta.Headers.Get("ETag")) {
		return
	}

	cacheReadResult, err := h.cacheManager.Get(r.Context(), cacheKey)
	if err != nil || !cacheReadResult.Hit || cacheReadResult.Content == nil {
		h.logger.Error().Err(err).Str("key", cacheKey).Msg("Failed to re-open cache item for serving, or item vanished.")

		h.handleCacheMiss(w, r, cacheKey, meta.UpstreamURL, relativePath)
		return
	}
	defer cacheReadResult.Content.Close()

	if contentType := meta.Headers.Get("Content-Type"); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	} else {
		w.Header().Set("Content-Type", util.GetContentType(relativePath))
	}
	if !meta.ModTime.IsZero() {
		w.Header().Set("Last-Modified", meta.ModTime.UTC().Format(http.TimeFormat))
	}

	util.SelectProxyHeaders(w.Header(), meta.Headers)

	if r.Method == http.MethodHead {
		if meta.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	contentSeeker, ok := cacheReadResult.Content.(io.ReadSeeker)
	if ok {

		serveName := filepath.Base(relativePath)
		if serveName == "." || serveName == "/" {
			serveName = ""
		}

		http.ServeContent(w, r, serveName, meta.ModTime, contentSeeker)
	} else {

		if meta.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
		}
		w.WriteHeader(http.StatusOK)
		if _, err := io.Copy(w, cacheReadResult.Content); err != nil {
			if !util.IsClientDisconnectedError(err) {
				h.logger.Error().Err(err).Str("key", cacheKey).Msg("Error streaming non-seekable cache content to client.")
			}
		}
	}
}

func (h *RepositoryHandler) checkClientConditionalHeaders(w http.ResponseWriter, r *http.Request, resourceModTime time.Time, resourceETag string) bool {

	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if util.CompareETags(inm, resourceETag) {

			w.WriteHeader(http.StatusNotModified)
			return true
		}
	}

	if ims := r.Header.Get("If-Modified-Since"); ims != "" && r.Header.Get("If-None-Match") == "" {
		if t, err := http.ParseTime(ims); err == nil {

			if !resourceModTime.IsZero() && !resourceModTime.After(t) {
				w.WriteHeader(http.StatusNotModified)
				return true
			}
		}
	}

	return false
}
