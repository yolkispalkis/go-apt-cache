package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/fetch"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type Server struct {
	*http.Server
	cfg          *config.Config
	cacheManager cache.CacheManager
	fetcher      *fetch.Coordinator
}

func New(
	cfg *config.Config,
	cacheManager cache.CacheManager,
	fetcher *fetch.Coordinator,
) (*Server, error) {
	mux := http.NewServeMux()

	mux.HandleFunc("/", rootHandler(cfg))
	mux.HandleFunc("/status", statusHandler(cfg, cacheManager))

	registeredCount := 0
	for _, repo := range cfg.Repositories {
		if !repo.Enabled {
			logging.Info("Skipping disabled repository", "repository_name", repo.Name)
			continue
		}

		repoClosure := repo
		pathPrefix := "/" + strings.Trim(repoClosure.Name, "/") + "/"
		stripPrefixPath := "/" + strings.Trim(repoClosure.Name, "/")
		repoHandler := NewRepositoryHandler(repoClosure, cfg, cacheManager, fetcher)
		mux.Handle(pathPrefix, http.StripPrefix(stripPrefixPath, repoHandler))

		logging.Info("Registered handler for repository",
			"repository_name", repoClosure.Name,
			"path_prefix", pathPrefix,
			"strip_prefix", stripPrefixPath,
			"upstream_url", repoClosure.URL)
		registeredCount++
	}
	if registeredCount == 0 {
		logging.Warn("No repositories were enabled or registered. The proxy will only serve '/' and '/status'.")
	}

	var handler http.Handler = mux
	handler = RecoveryMiddleware(handler)
	handler = LoggingMiddleware(handler)

	httpServer := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.Duration(),
		IdleTimeout:       cfg.Server.IdleTimeout.Duration(),
	}

	return &Server{
		Server:       httpServer,
		cfg:          cfg,
		cacheManager: cacheManager,
		fetcher:      fetcher,
	}, nil
}

func rootHandler(cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		logging.Debug("Serving root HTML status page", "path", r.URL.Path)

		fmt.Fprint(w, `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Go APT Proxy</title>
  <style>body{font-family: sans-serif; padding: 1em;} li { margin-bottom: 0.5em; }</style>
</head>
<body>
  <h1>Go APT Proxy</h1>
  <p>Status: Running</p>
  <h2>Configured Repositories:</h2>
  <ul>`)

		if len(cfg.Repositories) > 0 {
			for _, repo := range cfg.Repositories {
				status := "Disabled"
				color := "gray"
				if repo.Enabled {
					status = "Enabled"
					color = "green"
				}
				repoPath := "/" + strings.Trim(repo.Name, "/") + "/"
				fmt.Fprintf(w, `<li><strong>%s</strong> (<span style="color:%s;">%s</span>): <a href="%s">%s</a> ← %s</li>`,
					repo.Name, color, status, repoPath, repoPath, repo.URL)
			}
		} else {
			fmt.Fprint(w, "<li>No repositories configured.</li>")
		}

		fmt.Fprintf(w, `</ul>
  <p><a href="/status">View Detailed Status (Text)</a></p>
  <p><a href="/status?format=json">View Detailed Status (JSON)</a></p>
</body>
</html>`)
	}
}

func statusHandler(cfg *config.Config, cacheManager cache.CacheManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			logging.Warn("Method not allowed for status endpoint", "method", r.Method)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}

		stats := cacheManager.Stats()
		format := r.URL.Query().Get("format")

		if format == "json" {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			if err := enc.Encode(stats); err != nil {
				logging.ErrorE("Failed to encode status to JSON", err)
				w.Header().Del("Content-Type")
				http.Error(w, `{"error": "Failed to generate JSON status"}`, http.StatusInternalServerError)
			}
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		logging.Debug("Serving text status", "path", r.URL.Path)

		fmt.Fprintln(w, "--- Go APT Proxy Status ---")
		fmt.Fprintf(w, "Server Time: %s\n\n", time.Now().Format(time.RFC3339))

		fmt.Fprintln(w, "--- Cache Status ---")
		fmt.Fprintf(w, "Cache Enabled:          %t\n", stats.CacheEnabled)
		if stats.CacheEnabled {
			fmt.Fprintf(w, "Cache Directory:        %s\n", stats.CacheDirectory)
			fmt.Fprintf(w, "Clean on Start:         %t\n", stats.CleanOnStart)
			fmt.Fprintf(w, "Init Time:              %d ms\n", stats.InitTimeMs)
			if stats.InitError != "" {
				fmt.Fprintf(w, "Init Error:             %s\n", stats.InitError)
			}
			fmt.Fprintf(w, "Cached Items (memory):  %d\n", stats.ItemCount)
			fmt.Fprintf(w, "Current Cache Size:     %s (%d bytes)\n", util.FormatSize(stats.CurrentSize), stats.CurrentSize)
			fmt.Fprintf(w, "Max Cache Size:         %s (%d bytes)\n", util.FormatSize(stats.MaxSize), stats.MaxSize)
			fmt.Fprintf(w, "Cache Hits:             %d\n", stats.Hits)
			fmt.Fprintf(w, "Cache Misses:           %d\n", stats.Misses)
			fmt.Fprintln(w, "\n--- Cache Validation ---")
			fmt.Fprintf(w, "Validation TTL Enabled: %t\n", stats.ValidationTTLEnabled)
			if stats.ValidationTTLEnabled {
				fmt.Fprintf(w, "Validation TTL:         %s\n", stats.ValidationTTL)
				fmt.Fprintf(w, "Validation Entries:     %d\n", stats.ValidationItemCount)
				fmt.Fprintf(w, "Validation Hits:        %d\n", stats.ValidationHits)
				fmt.Fprintf(w, "Validation Errors:      %d\n", stats.ValidationErrors)
			}
			fmt.Fprintln(w, "\n--- Cache Inconsistencies (Counters) ---")
			fmt.Fprintf(w, "  Meta w/o Content:     %d\n", stats.InconsistencyMetaWithoutContent)
			fmt.Fprintf(w, "  Content w/o Meta:     %d\n", stats.InconsistencyContentWithoutMeta)
			fmt.Fprintf(w, "  Size Mismatch:        %d\n", stats.InconsistencySizeMismatch)
			fmt.Fprintf(w, "  Corrupt Metadata:     %d\n", stats.InconsistencyCorruptMetadata)
		}
		fmt.Fprintln(w, "")

		fmt.Fprintln(w, "--- Configured Repositories ---")
		if len(cfg.Repositories) > 0 {
			fmt.Fprintf(w, "%-15s | %-8s | %s\n", "Name", "Status", "Upstream URL")
			fmt.Fprintf(w, "%s\n", strings.Repeat("-", 60))
			for _, repo := range cfg.Repositories {
				status := "Disabled"
				if repo.Enabled {
					status = "Enabled"
				}
				fmt.Fprintf(w, "%-15s | %-8s | %s\n", repo.Name, status, repo.URL)
			}
		} else {
			fmt.Fprintln(w, "No repositories configured.")
		}
		fmt.Fprintln(w, "\n--- Status End ---")
	}
}

type RepositoryHandler struct {
	repoConfig           config.Repository
	serverConfig         config.ServerConfig
	cacheConfig          config.CacheConfig
	cacheManager         cache.CacheManager
	fetcher              *fetch.Coordinator
	skipValidationExtMap map[string]struct{}
}

func NewRepositoryHandler(
	repo config.Repository,
	cfg *config.Config,
	cacheMgr cache.CacheManager,
	fetcher *fetch.Coordinator,
) *RepositoryHandler {
	skipMap := make(map[string]struct{})
	if cfg.Cache.Enabled {
		for _, ext := range cfg.Cache.SkipValidationExtensions {
			normalizedExt := strings.ToLower(ext)
			if normalizedExt != "" {
				skipMap[normalizedExt] = struct{}{}
			}
		}
	}

	return &RepositoryHandler{
		repoConfig:           repo,
		serverConfig:         cfg.Server,
		cacheConfig:          cfg.Cache,
		cacheManager:         cacheMgr,
		fetcher:              fetcher,
		skipValidationExtMap: skipMap,
	}
}

func (h *RepositoryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	relativePath, requestCacheKey, upstreamURL, err := h.validateAndPrepareRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	logging.Debug("Processing repository request",
		"repo", h.repoConfig.Name,
		"relative_path_for_key", relativePath,
		"request_cache_key", requestCacheKey,
		"upstream_url", upstreamURL,
		"method", r.Method,
		"remote_addr", r.RemoteAddr,
	)

	cacheReader, cacheMeta, cacheGetErr := h.cacheManager.Get(r.Context(), requestCacheKey)

	switch {
	case cacheGetErr == nil:
		h.handleCacheHit(w, r, cacheReader, cacheMeta, requestCacheKey, relativePath, upstreamURL)
	case errors.Is(cacheGetErr, os.ErrNotExist):
		h.handleCacheMiss(w, r, requestCacheKey, relativePath, upstreamURL)
	default:
		h.cacheManager.RecordMiss()
		logging.ErrorE("Error reading from cache (not ErrNotExist)", cacheGetErr, "key", requestCacheKey)
		http.Error(w, "Internal Cache Error", http.StatusInternalServerError)
	}
}

func (h *RepositoryHandler) validateAndPrepareRequest(r *http.Request) (relativePath, requestCacheKey, upstreamURL string, err error) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		logging.Warn("Method not allowed for repository path", "method", r.Method, "repo", h.repoConfig.Name, "path", r.URL.Path)
		err = fmt.Errorf("%s: %w", http.StatusText(http.StatusMethodNotAllowed), net.ErrWriteToConnected)
		return
	}

	relativePath = util.CleanPath(strings.TrimPrefix(r.URL.Path, "/"))
	if strings.HasPrefix(relativePath, "..") || strings.Contains(relativePath, "/../") || strings.HasSuffix(relativePath, "/..") {
		err = errors.New("potential path traversal detected")
		logging.Warn("Bad request path", "error", err, "original_request_uri", r.RequestURI, "repo", h.repoConfig.Name)
		return
	}
	if relativePath == "." || relativePath == "/" {
		relativePath = ""
	}

	if relativePath == "" {
		requestCacheKey = h.repoConfig.Name
	} else {
		requestCacheKey = filepath.ToSlash(filepath.Join(h.repoConfig.Name, relativePath))
	}

	upstreamPathPart := strings.TrimPrefix(r.URL.Path, "/")
	upstreamBaseURL := strings.TrimSuffix(h.repoConfig.URL, "/")
	upstreamURL = upstreamBaseURL + "/" + upstreamPathPart

	return relativePath, requestCacheKey, upstreamURL, nil
}

func (h *RepositoryHandler) handleCacheHit(w http.ResponseWriter, r *http.Request, cacheReader io.ReadCloser, cacheMeta *cache.CacheMetadata, requestCacheKey, relativePath, upstreamURL string) {
	defer func() {
		if cacheReader != nil {
			_ = cacheReader.Close()
		}
	}()

	h.cacheManager.RecordHit()
	logging.Debug("Disk cache hit", "key", requestCacheKey)

	fileExt := strings.ToLower(filepath.Ext(relativePath))
	if _, exists := h.skipValidationExtMap[fileExt]; exists && fileExt != "" {
		logging.Debug("Skipping upstream validation based on file extension", "key", requestCacheKey, "extension", fileExt)
		w.Header().Set("X-Cache-Status", "HIT_NO_VALIDATION")
		h.serveDirectlyFromCache(w, r, cacheReader, cacheMeta, relativePath)
		cacheReader = nil
		return
	}

	w.Header().Set("X-Cache-Status", "HIT")
	action, _, err := h.attemptRevalidation(w, r, cacheMeta, requestCacheKey, relativePath, upstreamURL)

	switch action {
	case actionServeStale:
		logging.Debug("Serving stale content after revalidation check", "key", requestCacheKey, "revalidation_error", err)
		h.serveDirectlyFromCache(w, r, cacheReader, cacheMeta, relativePath)
		cacheReader = nil
	case actionServeNew:
		logging.Debug("Served new content after revalidation fetch", "key", requestCacheKey)
	case actionError:
		logging.Debug("Error during revalidation, cannot serve stale", "key", requestCacheKey, "error", err)
		h.handleFetchError(w, r, err, "Revalidation fetch failed", upstreamURL, requestCacheKey)
	}
}

func (h *RepositoryHandler) handleCacheMiss(w http.ResponseWriter, r *http.Request, requestCacheKey, relativePath, upstreamURL string) {
	h.cacheManager.RecordMiss()
	logging.Debug("Cache miss, fetching from upstream", "key", requestCacheKey, "upstream_url", upstreamURL)
	w.Header().Set("X-Cache-Status", "MISS")
	h.fetchAndServe(w, r, requestCacheKey, relativePath, upstreamURL, nil)
}

func (h *RepositoryHandler) fetchAndServe(w http.ResponseWriter, r *http.Request, cacheKey, relativePath, upstreamURL string, clientHeaders http.Header) {
	fetchResult, fetchErr := h.fetcher.Fetch(r.Context(), cacheKey, upstreamURL, clientHeaders)

	if fetchErr != nil {
		if errors.Is(fetchErr, fetch.ErrUpstreamNotModified) {
			h.cacheManager.RecordValidationError()
			logging.ErrorE("Unexpected upstream 304 during fetch (likely internal error or race)", fetchErr, "key", cacheKey, "url", upstreamURL)
			http.Error(w, "Internal Server Error (upstream protocol error)", http.StatusInternalServerError)
		} else {
			h.handleFetchError(w, r, fetchErr, "Fetch failed", upstreamURL, cacheKey)
		}
		return
	}

	logging.Debug("Fetch successful. Streaming to client and cache.", "key", cacheKey, "status_code_from_upstream", fetchResult.StatusCode)
	h.streamAndCache(w, r, fetchResult, cacheKey, relativePath)
}

func (h *RepositoryHandler) attemptRevalidation(w http.ResponseWriter, r *http.Request, cacheMeta *cache.CacheMetadata, requestCacheKey, relativePath, upstreamURL string) (revalidationAction, *fetch.Result, error) {
	validationTime, validationOK := h.cacheManager.GetValidation(requestCacheKey)
	if validationOK {
		logging.Debug("Validation cache hit, cache item is considered fresh.", "key", requestCacheKey, "validated_at", validationTime.Format(time.RFC3339))
		w.Header().Set("X-Cache-Status", "HIT_VALIDATED")
		return actionServeStale, nil, nil
	}

	logging.Debug("Validation cache expired or missing. Initiating revalidation fetch.", "key", requestCacheKey)
	w.Header().Set("X-Cache-Status", "REVALIDATING")

	revalidationHeaders := make(http.Header)
	if !cacheMeta.ModTime.IsZero() {
		revalidationHeaders.Set("If-Modified-Since", cacheMeta.ModTime.UTC().Format(http.TimeFormat))
	}
	etag := cacheMeta.Headers.Get("ETag")
	if etag != "" {
		revalidationHeaders.Set("If-None-Match", etag)
	}

	revalFlightKey := requestCacheKey + "_reval"
	revalResult, revalErr := h.fetcher.Fetch(r.Context(), revalFlightKey, upstreamURL, revalidationHeaders)

	if revalErr != nil {
		if errors.Is(revalErr, fetch.ErrUpstreamNotModified) {
			logging.Info("Revalidation successful: Upstream returned 304 Not Modified, cache is still valid", "url", upstreamURL, "key", requestCacheKey)
			h.cacheManager.PutValidation(requestCacheKey, time.Now())
			w.Header().Set("X-Cache-Status", "VALIDATED")
			return actionServeStale, nil, nil
		} else {
			h.cacheManager.RecordValidationError()
			return actionError, nil, revalErr
		}
	} else {
		logging.Info("Revalidation fetch successful: Upstream has newer content (sent 200 OK)", "key", requestCacheKey, "url", upstreamURL)
		h.streamAndCache(w, r, revalResult, requestCacheKey, relativePath)
		return actionServeNew, revalResult, nil
	}
}

func (h *RepositoryHandler) serveDirectlyFromCache(w http.ResponseWriter, r *http.Request, cacheReader io.ReadCloser, cacheMeta *cache.CacheMetadata, relativePath string) {
	defer cacheReader.Close()

	if h.checkClientCacheHeaders(w, r, cacheMeta.ModTime, cacheMeta.Headers.Get("ETag")) {
		return
	}
	h.serveFromCache(w, r, cacheReader, cacheMeta, relativePath)
}

func (h *RepositoryHandler) serveFromCache(w http.ResponseWriter, r *http.Request, cacheReader io.ReadCloser, cacheMeta *cache.CacheMetadata, relativePath string) {
	contentType := cacheMeta.Headers.Get("Content-Type")
	if contentType == "" {
		logging.Warn("Content-Type missing or empty in cached metadata during serve. Using fallback.", "key", cacheMeta.Key)
		contentType = util.GetContentType(relativePath)
		if contentType == "" {
			contentType = "application/octet-stream"
		}
	}
	w.Header().Set("Content-Type", contentType)

	if !cacheMeta.ModTime.IsZero() {
		w.Header().Set("Last-Modified", cacheMeta.ModTime.UTC().Format(http.TimeFormat))
	}

	util.ApplyCacheHeaders(w.Header(), cacheMeta.Headers)

	if r.Method == http.MethodHead {
		if cacheMeta.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(cacheMeta.Size, 10))
		} else {
			w.Header().Del("Content-Length")
			logging.Warn("HEAD request for cached item with unknown size", "key", cacheMeta.Key)
		}
		w.WriteHeader(http.StatusOK)
		logging.Debug("Served HEAD request from cache helper", "key", cacheMeta.Key)
		return
	}

	readSeeker, isSeeker := cacheReader.(io.ReadSeeker)
	if !isSeeker {
		logging.Warn("Cache reader is not io.ReadSeeker in serveFromCache, serving via io.Copy", "key", cacheMeta.Key)
		if cacheMeta.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(cacheMeta.Size, 10))
		}
		w.WriteHeader(http.StatusOK)
		bytesWritten, copyErr := io.Copy(w, cacheReader)
		if copyErr != nil && !isClientDisconnectedError(copyErr) {
			logging.ErrorE("Failed to write response body from non-seeker cache in helper", copyErr, "key", cacheMeta.Key, "written_bytes", bytesWritten)
		} else if copyErr == nil {
			logging.Debug("Served bytes from non-seeker cache via io.Copy in helper", "key", cacheMeta.Key, "written_bytes", bytesWritten)
		}
	} else {
		serveName := filepath.Base(relativePath)
		if serveName == "." || serveName == "/" || serveName == "" {
			serveName = h.repoConfig.Name
		}
		logging.Debug("Serving cache hit via ServeContent in helper", "key", cacheMeta.Key, "serve_name", serveName)
		http.ServeContent(w, r, serveName, cacheMeta.ModTime, readSeeker)
	}
}

func (h *RepositoryHandler) streamAndCache(w http.ResponseWriter, r *http.Request, fetchResult *fetch.Result, cacheKey, relativePath string) {
	defer fetchResult.Body.Close()

	cachePutMeta := cache.CacheMetadata{
		Version:   cache.MetadataVersion,
		FetchTime: time.Now().UTC(),
		ModTime:   fetchResult.ModTime,
		Size:      fetchResult.Size,
		Headers:   make(http.Header),
		Key:       cacheKey,
	}

	finalContentType := ""
	upstreamContentType := fetchResult.Header.Get("Content-Type")
	if upstreamContentType != "" && !strings.HasPrefix(strings.ToLower(upstreamContentType), "application/octet-stream") {
		finalContentType = upstreamContentType
	} else {
		finalContentType = util.GetContentType(relativePath)
		logging.Debug("Using path-detected Content-Type for cache/response", "path", relativePath, "detected_type", finalContentType, "upstream_type", upstreamContentType)
	}
	if finalContentType == "" {
		finalContentType = "application/octet-stream"
	}
	cachePutMeta.Headers.Set("Content-Type", finalContentType)
	w.Header().Set("Content-Type", finalContentType)

	util.SelectCacheControlHeaders(cachePutMeta.Headers, fetchResult.Header)

	pr, pw := io.Pipe()

	cacheErrChan := make(chan error, 1)
	cacheWriteDone := make(chan struct{})

	go func() {
		var putErr error
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			logging.Debug("Cache write goroutine finished", "key", cacheKey, "error", putErr, "duration", util.FormatDuration(duration))
			cacheErrChan <- putErr
			close(cacheWriteDone)
		}()

		logging.Debug("Cache write goroutine started", "key", cacheKey)
		putErr = h.cacheManager.Put(context.Background(), cacheKey, pr, cachePutMeta)
		if putErr == nil {
			h.cacheManager.PutValidation(cacheKey, time.Now())
		} else {
			_ = pr.CloseWithError(putErr)
		}
	}()

	util.ApplyCacheHeaders(w.Header(), fetchResult.Header)
	if !fetchResult.ModTime.IsZero() {
		w.Header().Set("Last-Modified", fetchResult.ModTime.UTC().Format(http.TimeFormat))
	}

	isChunked := len(fetchResult.Header.Values("Transfer-Encoding")) > 0 && fetchResult.Header.Get("Transfer-Encoding") != "identity"
	if fetchResult.Size >= 0 && !isChunked && fetchResult.StatusCode == http.StatusOK {
		w.Header().Set("Content-Length", strconv.FormatInt(fetchResult.Size, 10))
	} else if r.Method == http.MethodHead && fetchResult.Size < 0 {
		w.Header().Del("Content-Length")
	}

	currentCacheStatus := w.Header().Get("X-Cache-Status")
	if currentCacheStatus == "MISS" {
		w.Header().Set("X-Cache-Status", "MISS_SERVED_FRESH")
	} else {
		w.Header().Set("X-Cache-Status", "UPDATED_SERVED_FRESH")
	}

	w.WriteHeader(fetchResult.StatusCode)

	if r.Method == http.MethodHead {
		logging.Debug("Handling HEAD request in streamAndCache", "key", cacheKey)
		if err := pw.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			logging.Warn("Error closing pipe writer for HEAD request", "error", err, "key", cacheKey)
		}
	} else {
		logging.Debug("Starting TeeReader copy to client and cache pipe", "key", cacheKey)
		teeReader := io.TeeReader(fetchResult.Body, pw)
		bytesWritten, copyErr := io.Copy(w, teeReader)

		logging.Debug("Finished TeeReader copy to client, closing pipe writer", "key", cacheKey, "bytes_written", bytesWritten, "copy_error", copyErr)

		if err := pw.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			logging.Warn("Error closing pipe writer after TeeReader copy", "error", err, "key", cacheKey)
		}

		if copyErr != nil && !isClientDisconnectedError(copyErr) {
			logging.Warn("Error copying response body to client", "error", copyErr, "key", cacheKey, "written_bytes", bytesWritten)
		} else if copyErr == nil {
			logging.Debug("Successfully served bytes from upstream via TeeReader", "key", cacheKey, "written_bytes", bytesWritten)
		}
	}

	logging.Debug("Waiting for cache write goroutine to finish", "key", cacheKey)
	cacheWriteErr := <-cacheErrChan
	<-cacheWriteDone

	if cacheWriteErr != nil {
		logging.Warn("Cache write for key finished with error (client response might be complete or incomplete)", "key", cacheKey, "error", cacheWriteErr)
	} else {
		logging.Debug("Cache write confirmation received successfully", "key", cacheKey)
	}
}

func (h *RepositoryHandler) handleFetchError(w http.ResponseWriter, r *http.Request, fetchErr error, contextMsg, upstreamURL, requestCacheKey string) {
	logFields := []any{"url", upstreamURL, "key", requestCacheKey, "repo", h.repoConfig.Name, "error", fetchErr}

	var netErr net.Error
	isNetTimeout := errors.As(fetchErr, &netErr) && netErr.Timeout()

	switch {
	case errors.Is(fetchErr, fetch.ErrNotFound):
		logging.Warn(contextMsg+": Upstream resource not found (404)", logFields...)
		http.NotFound(w, r)

	case errors.Is(fetchErr, fetch.ErrUpstreamNotModified):
		logging.ErrorE(contextMsg+": Unexpected 304 Not Modified from upstream", fetchErr, logFields...)
		http.Error(w, "Internal Server Error (Unexpected 304)", http.StatusInternalServerError)

	case errors.Is(fetchErr, context.Canceled), errors.Is(fetchErr, context.DeadlineExceeded):
		logging.Warn(contextMsg+": Request context cancelled or deadline exceeded", logFields...)
		select {
		case <-r.Context().Done():
			logging.Debug("Client context done, not sending error response", "key", requestCacheKey)
		default:
			http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
		}

	case isNetTimeout:
		logging.ErrorE(contextMsg+": Upstream request timeout", fetchErr, logFields...)
		http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)

	default:
		logging.ErrorE(contextMsg+": Bad gateway or upstream connection error", fetchErr, logFields...)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
	}
}

func (h *RepositoryHandler) checkClientCacheHeaders(w http.ResponseWriter, r *http.Request, modTime time.Time, etag string) bool {
	clientETag := r.Header.Get("If-None-Match")
	if clientETag != "" && etag != "" {
		if util.CompareETags(clientETag, etag) {
			logging.Debug("Client cache check: ETag match", "client_if_none_match", clientETag, "cache_etag", etag, "path", r.URL.Path)
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	}

	clientModSince := r.Header.Get("If-Modified-Since")
	if clientModSince != "" && !modTime.IsZero() {
		if t, err := http.ParseTime(clientModSince); err == nil {
			if !modTime.Truncate(time.Second).After(t.Truncate(time.Second)) {
				logging.Debug("Client cache check: Not modified since", "client_if_modified_since", clientModSince, "path", r.URL.Path, "cache_mod_time", modTime.UTC().Format(http.TimeFormat))
				w.WriteHeader(http.StatusNotModified)
				return true
			}
		} else {
			logging.Warn("Could not parse If-Modified-Since header from client", "error", err, "if_modified_since_header", clientModSince, "path", r.URL.Path)
		}
	}

	return false
}

func isClientDisconnectedError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
		return true
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "client disconnected") ||
		strings.Contains(errStr, "request canceled") ||
		strings.Contains(errStr, "connection closed") ||
		strings.Contains(errStr, "write: broken pipe") ||
		strings.Contains(errStr, "tls: user canceled") ||
		strings.Contains(errStr, "stream canceled") ||
		strings.Contains(errStr, "context canceled")
}

type revalidationAction int

const (
	actionServeStale revalidationAction = iota
	actionServeNew
	actionError
)
