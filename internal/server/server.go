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

func New(cfg *config.Config, cacheManager cache.CacheManager) (*Server, error) {
	fetcher := fetch.NewCoordinator(
		cfg.Server.RequestTimeout.Duration(),
		cfg.Server.MaxConcurrentFetches,
		cacheManager,
		cfg.Server.UserAgent,
	)
	logging.Info("Fetch coordinator initialized",
		"max_concurrent", cfg.Server.MaxConcurrentFetches,
		"timeout", cfg.Server.RequestTimeout.Duration(),
		"user_agent", cfg.Server.UserAgent,
	)

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
		fmt.Fprint(w, `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>Go APT Proxy</title><style>body{font-family: sans-serif; padding: 1em;} li { margin-bottom: 0.5em; }</style></head><body><h1>Go APT Proxy</h1><p>Status: Running</p><h2>Configured Repositories:</h2><ul>`)
		if len(cfg.Repositories) > 0 {
			for _, repo := range cfg.Repositories {
				status := "Disabled"
				color := "gray"
				if repo.Enabled {
					status = "Enabled"
					color = "green"
				}
				repoPath := "/" + strings.Trim(repo.Name, "/") + "/"
				fmt.Fprintf(w, `<li><strong>%s</strong> (<span style="color:%s;">%s</span>): <a href="%s">%s</a> ← %s</li>`, repo.Name, color, status, repoPath, repoPath, repo.URL)
			}
		} else {
			fmt.Fprint(w, "<li>No repositories configured.</li>")
		}
		fmt.Fprint(w, `</ul><p><a href="/status">View Detailed Status (Text)</a></p><p><a href="/status?format=json">View Detailed Status (JSON)</a></p></body></html>`)
	}
}

func statusHandler(cfg *config.Config, cacheManager cache.CacheManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
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
				http.Error(w, `{"error": "Failed to generate JSON status"}`, http.StatusInternalServerError)
			}
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
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
			fmt.Fprintln(w, "\n--- Cache Inconsistencies (Counters Get/Scan) ---")
			fmt.Fprintf(w, "  Meta Missing (Get):     %d\n", stats.InconsistencyMetaMissingOnGet)
			fmt.Fprintf(w, "  Meta Read Err (Get):    %d\n", stats.InconsistencyMetaReadError)
			fmt.Fprintf(w, "  Content Missing (Get):  %d\n", stats.InconsistencyContentMissingOnGet)
			fmt.Fprintf(w, "  Content Open Err (Get): %d\n", stats.InconsistencyContentOpenError)
			fmt.Fprintf(w, "  Content Stat Err (Get): %d\n", stats.InconsistencyContentStatError)
			fmt.Fprintf(w, "  Size Mismatch (Get):    %d\n", stats.InconsistencySizeMismatchOnGet)
			fmt.Fprintf(w, "  During Scan (approx):   %d\n", stats.InconsistencyDuringScan)

		} else {
			fmt.Fprintln(w, "Cache is disabled.")
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

func NewRepositoryHandler(repo config.Repository, cfg *config.Config, cacheMgr cache.CacheManager, fetcher *fetch.Coordinator) *RepositoryHandler {
	skipMap := make(map[string]struct{})
	if cfg.Cache.Enabled {
		for _, ext := range cfg.Cache.SkipValidationExtensions {
			normalizedExt := strings.ToLower(ext)
			if normalizedExt != "" && strings.HasPrefix(normalizedExt, ".") {
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
		status := http.StatusBadRequest
		if errors.Is(err, net.ErrWriteToConnected) {
			status = http.StatusMethodNotAllowed
		} else if strings.Contains(err.Error(), "potential path traversal") {
			status = http.StatusBadRequest
		}
		http.Error(w, http.StatusText(status), status)
		return
	}

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
		return "", "", "", fmt.Errorf("method %s not allowed: %w", r.Method, net.ErrWriteToConnected)
	}

	upstreamPathPart := strings.TrimPrefix(r.URL.Path, "/")

	cleanedLocalPath := util.CleanPath(upstreamPathPart)
	if strings.HasPrefix(cleanedLocalPath, "..") || strings.Contains(cleanedLocalPath, "../") {
		return "", "", "", errors.New("potential path traversal detected")
	}

	relativePath = cleanedLocalPath
	if relativePath == "." || relativePath == "/" {
		relativePath = ""
	}

	if relativePath == "" {
		requestCacheKey = h.repoConfig.Name
	} else {

		requestCacheKey = filepath.ToSlash(filepath.Join(h.repoConfig.Name, relativePath))
	}

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
	_, skipValidation := h.skipValidationExtMap[fileExt]

	if skipValidation && fileExt != "" {
		logging.Debug("Skipping upstream validation based on file extension", "key", requestCacheKey, "extension", fileExt)
		w.Header().Set("X-Cache-Status", "HIT_NO_VALIDATION")
		h.serveFromCache(w, r, cacheReader, cacheMeta, relativePath)
		return
	}

	serveStale, err := h.performRevalidation(w, r, cacheMeta, requestCacheKey, relativePath, upstreamURL)
	if err != nil {
		if !errors.Is(err, fetch.ErrUpstreamNotModified) && !errors.Is(err, fetch.ErrNotFound) && !isClientDisconnectedError(err) {

			logging.Debug("Revalidation failed or handled, response sent.", "error_type", fmt.Sprintf("%T", err), "key", requestCacheKey)
		}
		return
	}

	if serveStale {
		logging.Debug("Serving stale content after revalidation attempt", "key", requestCacheKey)
		w.Header().Set("X-Cache-Status", "HIT_STALE")
		h.serveFromCache(w, r, cacheReader, cacheMeta, relativePath)
	} else {

		logging.Debug("Revalidation successful or updated content served", "key", requestCacheKey)
	}
}

func (h *RepositoryHandler) handleCacheMiss(w http.ResponseWriter, r *http.Request, requestCacheKey, relativePath, upstreamURL string) {
	h.cacheManager.RecordMiss()
	logging.Debug("Cache miss, fetching from upstream and caching", "key", requestCacheKey, "upstream_url", upstreamURL)
	w.Header().Set("X-Cache-Status", "MISS")

	fetchErr := h.fetcher.FetchAndCache(r.Context(), requestCacheKey, upstreamURL, relativePath, r.Header)

	if errors.Is(fetchErr, fetch.ErrUpstreamNotModified) {
		logging.Info("Cache miss fetch resulted in 304 (client likely up-to-date), returning 304", "key", requestCacheKey)
		w.Header().Set("X-Cache-Status", "MISS_CLIENT_NOT_MODIFIED")
		w.WriteHeader(http.StatusNotModified)
		return
	}

	if fetchErr != nil {
		h.handleFetchError(w, r, fetchErr, upstreamURL, requestCacheKey)
		return
	}

	w.Header().Set("X-Cache-Status", "MISS_SERVED_CACHE")
	cacheReader, cacheMeta, cacheGetErr := h.cacheManager.Get(r.Context(), requestCacheKey)
	if cacheGetErr != nil {
		logging.ErrorE("CRITICAL: Failed to read from cache immediately after successful FetchAndCache", cacheGetErr, "key", requestCacheKey)
		h.handleFetchError(w, r, fmt.Errorf("post-fetch cache read error: %w", cacheGetErr), upstreamURL, requestCacheKey)
		return
	}
	defer cacheReader.Close()

	h.serveFromCache(w, r, cacheReader, cacheMeta, relativePath)
}

func (h *RepositoryHandler) performRevalidation(w http.ResponseWriter, r *http.Request, cacheMeta *cache.CacheMetadata, requestCacheKey, relativePath, upstreamURL string) (serveStale bool, err error) {
	if h.checkValidationCache(requestCacheKey) {
		w.Header().Set("X-Cache-Status", "HIT_VALIDATED")
		if h.checkClientCacheHeaders(w, r, cacheMeta.ModTime, cacheMeta.Headers.Get("ETag")) {
			return false, nil
		}
		return true, nil
	}

	logging.Debug("Validation cache expired or missing. Initiating revalidation fetch.", "key", requestCacheKey)
	w.Header().Set("X-Cache-Status", "REVALIDATING")

	newFetchResult, revalErr := h.executeUpstreamRevalidation(r.Context(), requestCacheKey, upstreamURL, cacheMeta)

	serveStale, handled, err := h.handleRevalidationResult(w, r, revalErr, newFetchResult, cacheMeta, requestCacheKey, relativePath, upstreamURL)

	if handled {
		return false, err
	}

	return serveStale, err
}

func (h *RepositoryHandler) checkValidationCache(requestCacheKey string) bool {
	validationTime, validationOK := h.cacheManager.GetValidation(requestCacheKey)
	if validationOK {
		logging.Debug("Validation cache hit, cache item is considered fresh.", "key", requestCacheKey, "validated_at", validationTime.Format(time.RFC3339))
		return true
	}
	return false
}

func (h *RepositoryHandler) executeUpstreamRevalidation(ctx context.Context, requestCacheKey, upstreamURL string, cacheMeta *cache.CacheMetadata) (*fetch.FetchResult, error) {
	revalidationHeaders := make(http.Header)
	if !cacheMeta.ModTime.IsZero() {
		revalidationHeaders.Set("If-Modified-Since", cacheMeta.ModTime.UTC().Format(http.TimeFormat))
	}
	if etag := cacheMeta.Headers.Get("ETag"); etag != "" {
		revalidationHeaders.Set("If-None-Match", etag)
	}
	revalKey := requestCacheKey + "::reval"
	return h.fetcher.FetchForRevalidation(ctx, revalKey, upstreamURL, revalidationHeaders)
}

func (h *RepositoryHandler) handleRevalidationResult(w http.ResponseWriter, r *http.Request, revalErr error, newFetchResult *fetch.FetchResult, cacheMeta *cache.CacheMetadata, requestCacheKey, relativePath, upstreamURL string) (serveStale bool, handled bool, err error) {
	if newFetchResult != nil {
		defer newFetchResult.Body.Close()
	}

	if errors.Is(revalErr, fetch.ErrUpstreamNotModified) {
		logging.Info("Revalidation successful: Upstream returned 304 Not Modified", "key", requestCacheKey, "url", upstreamURL)
		h.cacheManager.PutValidation(requestCacheKey, time.Now())
		w.Header().Set("X-Cache-Status", "VALIDATED")
		if h.checkClientCacheHeaders(w, r, cacheMeta.ModTime, cacheMeta.Headers.Get("ETag")) {
			return false, true, revalErr
		}
		return true, false, nil
	}

	if revalErr != nil {
		h.cacheManager.RecordValidationError()
		logging.Warn("Revalidation fetch failed, serving stale content if possible", "error", revalErr, "key", requestCacheKey, "url", upstreamURL)
		w.Header().Set("X-Cache-Status", "REVALIDATION_FAILED_STALE")
		if errors.Is(revalErr, fetch.ErrNotFound) {
			logging.Warn("Upstream returned 404 during revalidation, deleting local cache entry", "key", requestCacheKey)
			_ = h.cacheManager.Delete(context.Background(), requestCacheKey)
			http.NotFound(w, r)
			return false, true, revalErr
		}

		if h.checkClientCacheHeaders(w, r, cacheMeta.ModTime, cacheMeta.Headers.Get("ETag")) {
			return false, true, nil
		}

		return true, false, nil
	}

	logging.Info("Revalidation fetch successful: Upstream has newer content (2xx received)", "key", requestCacheKey, "url", upstreamURL)

	h.fetchAndServe(w, r, requestCacheKey, relativePath, upstreamURL, "UPDATED_SERVED_CACHE")
	return false, true, nil
}

func (h *RepositoryHandler) fetchAndServe(w http.ResponseWriter, r *http.Request, requestCacheKey, relativePath, upstreamURL, successStatusHeader string) {

	clientHeaders := r.Header
	if successStatusHeader == "UPDATED_SERVED_CACHE" {

		clientHeaders = make(http.Header)

	}

	fetchErr := h.fetcher.FetchAndCache(r.Context(), requestCacheKey, upstreamURL, relativePath, clientHeaders)
	if fetchErr != nil {

		if errors.Is(fetchErr, fetch.ErrUpstreamNotModified) {
			logging.Warn("Unexpected 304 during fetchAndServe, sending 304", "key", requestCacheKey)
			w.Header().Set("X-Cache-Status", "UPDATE_RACE_304")
			w.WriteHeader(http.StatusNotModified)
			return
		}
		h.handleFetchError(w, r, fetchErr, upstreamURL, requestCacheKey)
		return
	}

	w.Header().Set("X-Cache-Status", successStatusHeader)
	cacheReader, cacheMeta, cacheGetErr := h.cacheManager.Get(r.Context(), requestCacheKey)
	if cacheGetErr != nil {
		logging.ErrorE("CRITICAL: Failed to read from cache immediately after successful fetchAndServe/FetchAndCache", cacheGetErr, "key", requestCacheKey)
		h.handleFetchError(w, r, fmt.Errorf("post-fetch cache read error: %w", cacheGetErr), upstreamURL, requestCacheKey)
		return
	}
	defer cacheReader.Close()

	h.serveFromCache(w, r, cacheReader, cacheMeta, relativePath)
}

func (h *RepositoryHandler) serveFromCache(w http.ResponseWriter, r *http.Request, cacheReader io.ReadCloser, cacheMeta *cache.CacheMetadata, relativePath string) {
	if h.checkClientCacheHeaders(w, r, cacheMeta.ModTime, cacheMeta.Headers.Get("ETag")) {
		return
	}

	contentType := cacheMeta.Headers.Get("Content-Type")
	if contentType == "" {
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
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	readSeeker, isSeeker := cacheReader.(io.ReadSeeker)
	if isSeeker {
		serveName := filepath.Base(relativePath)
		if serveName == "." || serveName == "/" || serveName == "" {
			serveName = h.repoConfig.Name
		}
		http.ServeContent(w, r, serveName, cacheMeta.ModTime, readSeeker)
	} else {

		logging.Warn("Cache reader is not io.ReadSeeker, serving via io.Copy", "key", cacheMeta.Key)
		if cacheMeta.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(cacheMeta.Size, 10))
		}
		w.WriteHeader(http.StatusOK)
		bytesWritten, copyErr := io.Copy(w, cacheReader)
		if copyErr != nil && !isClientDisconnectedError(copyErr) {
			logging.ErrorE("Failed to write response body from non-seeker cache", copyErr, "key", cacheMeta.Key, "written_bytes", bytesWritten)
		}
	}
}

func (h *RepositoryHandler) handleFetchError(w http.ResponseWriter, r *http.Request, fetchErr error, upstreamURL, requestCacheKey string) {
	logFields := []any{"url", upstreamURL, "key", requestCacheKey, "repo", h.repoConfig.Name, "error", fetchErr}

	status := http.StatusBadGateway
	msg := "Bad Gateway"

	var netErr net.Error
	isNetTimeout := errors.As(fetchErr, &netErr) && netErr.Timeout()

	switch {
	case errors.Is(fetchErr, fetch.ErrNotFound):
		status = http.StatusNotFound
		msg = http.StatusText(status)
		logging.Warn("Fetch Error: Upstream resource not found (404)", logFields...)

	case errors.Is(fetchErr, fetch.ErrCacheWriteFailed):
		status = http.StatusInternalServerError
		msg = "Internal Server Error (Cache Write Failure)"
		logging.ErrorE("Fetch Error: Failed to write to cache after fetch", fetchErr, logFields...)
	case errors.Is(fetchErr, context.Canceled), errors.Is(fetchErr, context.DeadlineExceeded), isNetTimeout:
		status = http.StatusGatewayTimeout
		msg = "Gateway Timeout"
		logging.Warn("Fetch Error: Request context cancelled or timeout", logFields...)
	default:
		logging.ErrorE("Fetch Error: Bad gateway or upstream connection error", fetchErr, logFields...)
	}

	select {
	case <-r.Context().Done():
		logging.Debug("Client context done, not sending fetch error response", "key", requestCacheKey, "original_error", fetchErr)
	default:

		if w.Header().Get("X-Cache-Status") == "" {
			http.Error(w, msg, status)
		} else {
			logging.Error("Could not send fetch error response, headers possibly already written", append(logFields, "status", status, "msg", msg)...)

		}
	}
}

func (h *RepositoryHandler) checkClientCacheHeaders(w http.ResponseWriter, r *http.Request, modTime time.Time, etag string) bool {

	clientETag := r.Header.Get("If-None-Match")
	if clientETag != "" && etag != "" {
		if util.CompareETags(clientETag, etag) {
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	}

	clientModSince := r.Header.Get("If-Modified-Since")
	if clientModSince != "" && !modTime.IsZero() {
		if t, err := http.ParseTime(clientModSince); err == nil {

			if !modTime.Truncate(time.Second).After(t.Truncate(time.Second)) {
				w.WriteHeader(http.StatusNotModified)
				return true
			}
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

	if errors.Is(err, http.ErrAbortHandler) {
		return true
	}
	errStr := strings.ToLower(err.Error())

	return strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "client disconnected") ||
		strings.Contains(errStr, "request canceled") ||
		strings.Contains(errStr, "connection closed") ||
		strings.Contains(errStr, "user canceled") ||
		strings.Contains(errStr, "context canceled") ||
		strings.Contains(errStr, "request body closed") ||
		strings.Contains(errStr, "handler aborted")
}
