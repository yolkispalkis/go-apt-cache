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
		repoCfg:  rcfg,
		srvCfg:   scfg,
		cacheCfg: ccfg,
		cm:       cm,
		fetcher:  fc,
		log:      parentLog.With().Str("repo", rcfg.Name).Logger(),
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
		if strings.HasPrefix(cleanedPath, "..") || cleanedPath == ".." ||
			(relPath != cleanedPath && strings.HasPrefix(cleanedPath, "../")) {
			h.log.Warn().Str("original_path", r.URL.Path).Str("rel_path", relPath).Str("cleaned_path", cleanedPath).Msg("Path traversal attempt detected and blocked")
			http.Error(w, "Invalid path (traversal attempt)", http.StatusBadRequest)
			return
		}
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

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	// Используем новый метод без контента для первоначальной проверки
	cacheRes, err := h.cm.GetWithOptions(r.Context(), cacheKey, cache.GetOptions{WithContent: false})
	if err != nil && !errors.Is(err, cache.ErrNotFound) {
		h.log.Error().Err(err).Str("key", cacheKey).Msg("Cache get error")
		http.Error(w, "Cache error", http.StatusInternalServerError)
		return
	}

	if cacheRes != nil && cacheRes.Hit {
		if h.log.GetLevel() <= zerolog.DebugLevel {
			h.log.Debug().Str("key", cacheKey).Str("status", "HIT").Msg("Cache hit")
		}
		h.handleCacheHit(w, r, cacheKey, upstreamURL, relPath, cacheRes)
		return
	}

	if h.log.GetLevel() <= zerolog.InfoLevel {
		h.log.Info().Str("key", cacheKey).Str("status", "MISS").Msg("Cache miss")
	}
	h.handleCacheMiss(w, r, cacheKey, upstreamURL)
}

func (h *RepoHandler) handleCacheHit(w http.ResponseWriter, r *http.Request,
	key, upstreamURL, relPath string, cacheRes *cache.GetResult) {

	meta := cacheRes.Meta
	if cacheRes.Content != nil {
		defer cacheRes.Content.Close()
	}

	if meta.StatusCode == http.StatusNotFound {
		if h.cacheCfg.NegativeTTL.StdDuration() > 0 && time.Since(meta.FetchedAt.Time()) > h.cacheCfg.NegativeTTL.StdDuration() {
			h.log.Info().Str("key", key).Msg("Expired negative cache entry, re-fetching.")
			go h.cm.Delete(context.Background(), key)
			h.handleCacheMiss(w, r, key, upstreamURL)
			return
		}
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	needsReval := false
	now := time.Now()

	if clientCC := util.ParseCacheControl(r.Header.Get("Cache-Control")); clientCC["no-cache"] != "" {
		h.log.Debug().Str("key", key).Msg("Client sent 'Cache-Control: no-cache', forcing revalidation.")
		needsReval = true
	}

	if !needsReval && meta.IsStale(now) {
		h.log.Debug().Str("key", key).Time("expiresAt", meta.ExpiresAt.Time()).Msg("Item is stale based on its expiresAt, needs revalidation.")
		needsReval = true
	}

	if needsReval {
		h.log.Info().Str("key", key).Msg("Revalidation triggered for cached item.")
		h.revalidateAndServe(w, r, key, upstreamURL, meta)
	} else {
		h.serveFromCache(w, r, key, relPath, meta)
	}
}

func (h *RepoHandler) revalidateAndServe(w http.ResponseWriter, r *http.Request,
	key, upstreamURL string, currentMeta *cache.ItemMeta) {

	fetchOpts := &fetch.Options{}
	if lmStr := currentMeta.Headers.Get("Last-Modified"); lmStr != "" {
		if t, errLMP := http.ParseTime(lmStr); errLMP == nil {
			fetchOpts.IfModSince = t
		}
	} else if !currentMeta.FetchedAt.Time().IsZero() && currentMeta.StatusCode == http.StatusOK {
		fetchOpts.IfModSince = currentMeta.FetchedAt.Time()
	}

	if etag := currentMeta.Headers.Get("ETag"); etag != "" {
		fetchOpts.IfNoneMatch = etag
	}

	h.log.Debug().Str("key", key).Str("upstreamURL", upstreamURL).
		Time("if_mod_since", fetchOpts.IfModSince).Str("if_none_match", fetchOpts.IfNoneMatch).
		Msg("Revalidating item with conditional fetch")

	derivedRelPath := ""
	if strings.HasPrefix(currentMeta.UpstreamURL, h.repoCfg.URL) {
		derivedRelPath = strings.TrimPrefix(currentMeta.UpstreamURL, h.repoCfg.URL)
	} else {
		parts := strings.SplitN(key, "/", 2)
		if len(parts) == 2 {
			derivedRelPath = parts[1]
		}
	}

	fetchRes, fetchErr := h.fetcher.Fetch(r.Context(), key, upstreamURL, fetchOpts)
	if fetchRes != nil && fetchRes.Body != nil {
		defer fetchRes.Body.Close()
	}

	if errors.Is(fetchErr, fetch.ErrUpstreamNotModified) {
		if fetchRes == nil || fetchRes.Header == nil {
			h.log.Error().Str("key", key).Msg("Internal error: 304 Not Modified but no headers available from fetcher.")
			h.serveFromCache(w, r, key, derivedRelPath, currentMeta)
			return
		}

		h.log.Info().Str("key", key).Msg("Revalidation returned 304 Not Modified. Updating meta and serving from cache.")
		validationTime := time.Now()
		updatedMeta, errUpdate := h.cm.UpdateAfterValidation(r.Context(), key, validationTime, fetchRes.Header, currentMeta.StatusCode, currentMeta.Size)
		if errUpdate != nil {
			h.log.Warn().Err(errUpdate).Str("key", key).Msg("Failed to update cache metadata after 304. Serving with old meta.")
			h.serveFromCache(w, r, key, derivedRelPath, currentMeta)
		} else {
			h.serveFromCache(w, r, key, derivedRelPath, updatedMeta)
		}
		return
	}

	if fetchErr == nil {
		h.log.Info().Str("key", key).Int("status", fetchRes.Status).Msg("Revalidation successful, got new content (2xx).")
		h.storeAndServe(w, r, key, upstreamURL, fetchRes)
		return
	}

	h.log.Warn().Err(fetchErr).Str("key", key).Msg("Revalidation fetch failed (not 304 or 2xx).")

	ccDirectives := util.ParseCacheControl(currentMeta.Headers.Get("Cache-Control"))
	_, mustRevalidate := ccDirectives["must-revalidate"]
	_, proxyRevalidate := ccDirectives["proxy-revalidate"]

	if mustRevalidate || proxyRevalidate {
		isNetErr := errors.Is(fetchErr, fetch.ErrNetwork) || errors.Is(fetchErr, context.DeadlineExceeded) || errors.Is(fetchErr, context.Canceled)
		isServerErr := errors.Is(fetchErr, fetch.ErrUpstreamServerErr)

		if isNetErr || isServerErr {
			h.log.Warn().Str("key", key).Msg("Item has must-revalidate/proxy-revalidate and revalidation failed due to upstream/network error. Returning 504.")
			http.Error(w, http.StatusText(http.StatusGatewayTimeout), http.StatusGatewayTimeout)
			return
		}
	}

	// Обрабатываем 404 Not Found отдельно и в первую очередь.
	if errors.Is(fetchErr, fetch.ErrUpstreamNotFound) {
		h.log.Warn().Str("key", key).Msg("Item not found on upstream during revalidation. Deleting from cache and returning 404.")
		go h.cm.Delete(context.Background(), key) // Удаляем асинхронно, чтобы не блокировать ответ.
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	// Для других ошибок (5xx, сетевые проблемы) мы можем отдать устаревший контент, если это разрешено.
	if errors.Is(fetchErr, fetch.ErrUpstreamClientErr) || errors.Is(fetchErr, fetch.ErrUpstreamServerErr) {
		h.log.Info().Str("key", key).Msg("Serving stale content as revalidation failed with upstream error (or no must-revalidate).")
		h.serveFromCache(w, r, key, derivedRelPath, currentMeta)
	} else {
		// Для непредвиденных ошибок (не 404, не 5xx, не сетевые)
		h.log.Error().Err(fetchErr).Str("key", key).Msg("Unhandled revalidation fetch error. Returning 500.")
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}

func (h *RepoHandler) handleCacheMiss(w http.ResponseWriter, r *http.Request,
	key, upstreamURL string) {

	fetchOpts := &fetch.Options{}
	if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, errIMS := http.ParseTime(ims); errIMS == nil {
			fetchOpts.IfModSince = t
		}
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		fetchOpts.IfNoneMatch = inm
	}

	fetchRes, fetchErr := h.fetcher.Fetch(r.Context(), key, upstreamURL, fetchOpts)
	if fetchRes != nil && fetchRes.Body != nil {
		defer fetchRes.Body.Close()
	}

	if fetchErr != nil {
		h.handleFetchError(w, key, upstreamURL, fetchErr, fetchRes)
		return
	}

	h.storeAndServe(w, r, key, upstreamURL, fetchRes)
}

func (h *RepoHandler) handleFetchError(w http.ResponseWriter, key, upstreamURL string, err error, fetchResOnError *fetch.Result) {
	if errors.Is(err, fetch.ErrUpstreamNotModified) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if errors.Is(err, fetch.ErrUpstreamNotFound) {
		if h.cacheCfg.NegativeTTL.StdDuration() > 0 {
			go func() {
				// ИСПРАВЛЕНО: Теперь передаем владение заголовком кешу, а не просто копируем
				var negativeHeaders http.Header
				if fetchResOnError != nil && fetchResOnError.Header != nil {
					negativeHeaders = fetchResOnError.Header
				} else {
					negativeHeaders = make(http.Header)
				}
				popts := cache.PutOptions{
					UpstreamURL: upstreamURL,
					StatusCode:  http.StatusNotFound,
					Headers:     negativeHeaders, // Передаем владение
					FetchedAt:   time.Now(),
					Size:        0,
				}
				if _, putErr := h.cm.Put(context.Background(), key, nil, popts); putErr != nil {
					h.log.Warn().Err(putErr).Str("key", key).Msg("Failed to store negative cache entry for 404.")
				} else {
					h.log.Debug().Str("key", key).Msg("Stored negative cache entry for 404.")
				}
			}()
		}
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	logCtx := h.log.Error().Err(err).Str("key", key).Str("upstreamURL", upstreamURL)
	status := http.StatusBadGateway // Статус по умолчанию

	if errors.Is(err, fetch.ErrNetwork) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		status = http.StatusGatewayTimeout
	} else if fetchResOnError != nil && fetchResOnError.Status > 0 {
		logCtx.Int("upstream_status", fetchResOnError.Status)
	}

	logCtx.Msg("Failed to fetch item from upstream on MISS.")
	http.Error(w, http.StatusText(status), status)
}

func (h *RepoHandler) storeAndServe(w http.ResponseWriter, r *http.Request,
	key, fetchedUpstreamURL string, fetchRes *fetch.Result) {

	// ИСПРАВЛЕНО: Заголовок берется из пула, но не возвращается здесь.
	// Владение передается в cm.Put, который отвечает за его жизненный цикл.
	headers := util.CopyHeader(fetchRes.Header)

	popts := cache.PutOptions{
		UpstreamURL: fetchedUpstreamURL,
		StatusCode:  fetchRes.Status,
		Headers:     headers, // Передаем владение
		Size:        fetchRes.Size,
		FetchedAt:   time.Now(),
	}

	if canonicalURL := fetchRes.Header.Get("X-Upstream-Url"); canonicalURL != "" {
		popts.UpstreamURL = canonicalURL
	} else if contentLoc := fetchRes.Header.Get("Content-Location"); contentLoc != "" {
		resolvedContentLoc, err := util.ResolveURL(fetchedUpstreamURL, contentLoc)
		if err == nil {
			popts.UpstreamURL = resolvedContentLoc
		} else {
			h.log.Warn().Str("key", key).Str("base_url", fetchedUpstreamURL).Str("content_location", contentLoc).Err(err).Msg("Failed to resolve Content-Location, using original fetch URL for cache metadata.")
		}
	}

	pr, pw := io.Pipe()
	var storeWg sync.WaitGroup
	var putErrGlobal error
	storeWg.Add(1)

	go func() {
		defer storeWg.Done()
		_, putErr := h.cm.Put(context.Background(), key, pr, popts)
		if putErr != nil {
			putErrGlobal = putErr
			if !errors.Is(putErr, io.ErrClosedPipe) &&
				!strings.Contains(putErr.Error(), "broken pipe") &&
				!strings.Contains(putErr.Error(), "read tcp") {
				h.log.Error().Err(putErr).Str("key", key).Msg("Error storing item to cache during Tee.")
			} else {
				h.log.Debug().Err(putErr).Str("key", key).Msg("Cache Put failed due to pipe closure, likely client disconnect.")
			}
		} else {
			h.log.Debug().Str("key", key).Msg("Item successfully stored in cache via Tee.")
		}
	}()

	teeBody := io.TeeReader(fetchRes.Body, pw)

	if h.checkClientConditional(w, r, fetchRes.ModTime, fetchRes.Header.Get("ETag")) {
		h.log.Debug().Str("key", key).Msg("Client conditional GET matched newly fetched resource (304). Discarding body, closing pipe to cache.")
		_, copyErr := io.Copy(io.Discard, teeBody)
		if copyErr != nil {
			_ = pw.CloseWithError(copyErr)
		} else {
			_ = pw.Close()
		}
		storeWg.Wait()
		return
	}

	util.CopyWhitelistedHeaders(w.Header(), fetchRes.Header)
	if !fetchRes.ModTime.IsZero() {
		w.Header().Set("Last-Modified", fetchRes.ModTime.UTC().Format(http.TimeFormat))
	}

	var clientCopyError error
	if r.Method == http.MethodHead {
		if fetchRes.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(fetchRes.Size, 10))
		}
		w.WriteHeader(fetchRes.Status)
		_, clientCopyError = io.Copy(io.Discard, teeBody)
	} else {
		if fetchRes.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(fetchRes.Size, 10))
		} else {
			w.Header().Del("Content-Length")
		}
		w.WriteHeader(fetchRes.Status)
		_, clientCopyError = io.Copy(w, teeBody)
	}

	if clientCopyError != nil {
		if !util.IsClientDisconnectedError(clientCopyError) {
			h.log.Error().Err(clientCopyError).Str("key", key).Msg("Error streaming response to client.")
		}
		_ = pw.CloseWithError(clientCopyError)
	} else {
		_ = pw.Close()
	}
	storeWg.Wait()

	if putErrGlobal != nil && util.IsClientDisconnectedError(clientCopyError) {
		if errors.Is(putErrGlobal, io.ErrClosedPipe) || strings.Contains(putErrGlobal.Error(), "broken pipe") || strings.Contains(putErrGlobal.Error(), "read tcp") {
			h.log.Debug().Err(putErrGlobal).Str("key", key).Msg("Cache store failed due to client disconnect (pipe closed), as expected after clientCopyError.")
		}
	}
}

func (h *RepoHandler) serveFromCache(w http.ResponseWriter, r *http.Request,
	key, relPath string, meta *cache.ItemMeta) {

	itemModTimeStr := meta.Headers.Get("Last-Modified")
	var modTimeForCheck time.Time
	if itemModTimeStr != "" {
		if t, errLMP := http.ParseTime(itemModTimeStr); errLMP == nil {
			modTimeForCheck = t
		}
	}

	if h.checkClientConditional(w, r, modTimeForCheck, meta.Headers.Get("ETag")) {
		h.log.Debug().Str("key", key).Msg("Serving 304 Not Modified from cache based on client conditionals.")
		return
	}

	// Теперь получаем с контентом
	cacheReadRes, err := h.cm.GetWithOptions(r.Context(), key, cache.GetOptions{WithContent: true})
	if err != nil || !cacheReadRes.Hit || cacheReadRes.Content == nil {
		if errors.Is(err, cache.ErrNotFound) {
			h.log.Warn().Err(err).Str("key", key).Msg("Cache item vanished before serving. Treating as miss.")
			h.handleCacheMiss(w, r, key, meta.UpstreamURL)
			return
		}
		h.log.Error().Err(err).Str("key", key).Msg("Failed to re-open cache item for serving. Treating as miss.")
		h.handleCacheMiss(w, r, key, meta.UpstreamURL)
		return
	}
	defer cacheReadRes.Content.Close()

	// Используем пул заголовков
	headers := util.CopyHeader(meta.Headers)
	defer util.ReturnHeader(headers)

	util.CopyWhitelistedHeaders(w.Header(), headers)
	if !modTimeForCheck.IsZero() {
		w.Header().Set("Last-Modified", modTimeForCheck.UTC().Format(http.TimeFormat))
	}

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
		if serveName == "." || serveName == "/" || serveName == "" {
			if relPath == "" {
				serveName = "index"
			} else {
				serveName = "resource"
			}
		}
		http.ServeContent(w, r, serveName, modTimeForCheck, contentSeeker)
	} else {
		if meta.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
		} else {
			w.Header().Del("Content-Length")
		}
		w.WriteHeader(meta.StatusCode)
		if _, copyErr := io.Copy(w, cacheReadRes.Content); copyErr != nil {
			if !util.IsClientDisconnectedError(copyErr) {
				h.log.Error().Err(copyErr).Str("key", key).Msg("Error streaming non-seekable cache content to client.")
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
		if t, errIMS := http.ParseTime(imsHdr); errIMS == nil {
			if !resModTime.IsZero() {
				if !resModTime.Truncate(time.Second).After(t.Truncate(time.Second)) {
					w.WriteHeader(http.StatusNotModified)
					return true
				}
			}
		}
	}
	return false
}
