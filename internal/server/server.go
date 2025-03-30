package server

import (
	"context"
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

		repoHandler := NewRepositoryHandler(repoClosure, cfg.Server, cacheManager, fetcher)

		handlerPath := "/" + strings.Trim(repoClosure.Name, "/")
		mux.Handle(pathPrefix, http.StripPrefix(handlerPath, repoHandler))

		logging.Info("Registered handler for repository",
			"repository_name", repoClosure.Name,
			"path_prefix", pathPrefix,
			"strip_prefix", handlerPath,
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

		for _, repo := range cfg.Repositories {
			status := "Disabled"
			color := "gray"
			if repo.Enabled {
				status = "Enabled"
				color = "green"
			}
			fmt.Fprintf(w, `<li><strong>%s</strong> (<span style="color:%s;">%s</span>): <a href="/%s/">/%s/</a> ← %s</li>`,
				repo.Name, color, status, repo.Name, repo.Name, repo.URL)
		}
		if len(cfg.Repositories) == 0 {
			fmt.Fprint(w, "<li>No repositories configured.</li>")
		}

		fmt.Fprintf(w, `</ul>
  <p><a href="/status">View Detailed Text Status</a></p>
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
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		cacheStats := cacheManager.Stats()
		logging.Debug("Serving text status", "path", r.URL.Path)

		fmt.Fprintln(w, "--- Go APT Proxy Status ---")
		fmt.Fprintf(w, "Server Time: %s\n\n", time.Now().Format(time.RFC3339))

		fmt.Fprintln(w, "--- Cache Status ---")
		fmt.Fprintf(w, "Cache Enabled:          %t\n", cacheStats.CacheEnabled)
		if cacheStats.CacheEnabled {
			fmt.Fprintf(w, "Cache Directory:        %s\n", cacheStats.CacheDirectory)
			fmt.Fprintf(w, "Cached Items (approx):  %d\n", cacheStats.ItemCount)
			fmt.Fprintf(w, "Current Cache Size:     %s (%d bytes)\n", util.FormatSize(cacheStats.CurrentSize), cacheStats.CurrentSize)
			fmt.Fprintf(w, "Max Cache Size:         %s (%d bytes)\n", util.FormatSize(cacheStats.MaxSize), cacheStats.MaxSize)
			fmt.Fprintf(w, "Validation TTL Enabled: %t\n", cacheStats.ValidationTTLEnabled)
			if cacheStats.ValidationTTLEnabled {
				fmt.Fprintf(w, "Validation TTL:         %s\n", cacheStats.ValidationTTL)
				fmt.Fprintf(w, "Validation Entries:     %d\n", cacheStats.ValidationItemCount)
			}
			fmt.Fprintln(w, "Cache Inconsistencies:")
			fmt.Fprintf(w, "  Meta w/o Content:     %d\n", cacheStats.InconsistencyMetaWithoutContent)
			fmt.Fprintf(w, "  Content w/o Meta:     %d\n", cacheStats.InconsistencyContentWithoutMeta)
			fmt.Fprintf(w, "  Size Mismatch:        %d\n", cacheStats.InconsistencySizeMismatch)
			fmt.Fprintf(w, "  Corrupt Metadata:     %d\n", cacheStats.InconsistencyCorruptMetadata)
		}
		fmt.Fprintln(w, "")

		fmt.Fprintln(w, "--- Configured Repositories ---")
		if len(cfg.Repositories) > 0 {
			for _, repo := range cfg.Repositories {
				status := "Disabled"
				if repo.Enabled {
					status = "Enabled"
				}
				fmt.Fprintf(w, "Name: %-15s Status: %-8s Upstream: %s\n", repo.Name, status, repo.URL)
			}
		} else {
			fmt.Fprintln(w, "No repositories configured.")
		}
		fmt.Fprintln(w, "\n--- Status End ---")
	}
}

type RepositoryHandler struct {
	repoConfig   config.Repository
	serverConfig config.ServerConfig
	cacheManager cache.CacheManager
	fetcher      *fetch.Coordinator
}

func NewRepositoryHandler(
	repo config.Repository,
	serverCfg config.ServerConfig,
	cacheMgr cache.CacheManager,
	fetcher *fetch.Coordinator,
) *RepositoryHandler {
	return &RepositoryHandler{
		repoConfig:   repo,
		serverConfig: serverCfg,
		cacheManager: cacheMgr,
		fetcher:      fetcher,
	}
}

func (h *RepositoryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		logging.Warn("Method not allowed for repository path", "method", r.Method, "repo", h.repoConfig.Name, "path", r.URL.Path)
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	relativePath := util.CleanPath(strings.TrimPrefix(r.URL.Path, "/"))

	if strings.HasPrefix(relativePath, "..") || strings.Contains(relativePath, "/../") || strings.HasSuffix(relativePath, "/..") {
		logging.Warn("Potential path traversal detected after strip/clean", "cleaned_relative_path", relativePath, "original_request_uri", r.RequestURI, "repo", h.repoConfig.Name)
		http.Error(w, "Bad Request: Invalid Path", http.StatusBadRequest)
		return
	}

	if relativePath == "." || relativePath == "/" {
		relativePath = ""
	}

	var requestCacheKey string
	if relativePath == "" {
		requestCacheKey = h.repoConfig.Name
	} else {
		requestCacheKey = filepath.ToSlash(filepath.Join(h.repoConfig.Name, relativePath))
	}

	upstreamPathPart := strings.TrimPrefix(r.URL.Path, "/")
	upstreamBaseURL := strings.TrimSuffix(h.repoConfig.URL, "/")
	upstreamURL := upstreamBaseURL + "/" + upstreamPathPart

	logging.Debug("Handling repository request",
		"repo", h.repoConfig.Name,
		"stripped_path", r.URL.Path,
		"relative_path_for_key", relativePath,
		"upstream_path_part", upstreamPathPart,
		"request_cache_key", requestCacheKey,
		"upstream_url", upstreamURL,
		"method", r.Method,
		"remote_addr", r.RemoteAddr,
	)

	if validationTime, ok := h.cacheManager.GetValidation(requestCacheKey); ok {
		logging.Debug("Validation cache hit, checking client If-Modified-Since/If-None-Match", "key", requestCacheKey, "validated_at", validationTime.Format(time.RFC3339))
	}

	cacheReader, cacheMeta, err := h.cacheManager.Get(r.Context(), requestCacheKey)

	if err == nil {
		defer cacheReader.Close()
		logging.Debug("Disk cache hit", "key", requestCacheKey)

		if h.checkClientCacheHeaders(w, r, cacheMeta.ModTime, cacheMeta.Headers.Get("ETag")) {
			return
		}

		util.ApplyCacheHeaders(w.Header(), cacheMeta.Headers)
		if !cacheMeta.ModTime.IsZero() {
			w.Header().Set("Last-Modified", cacheMeta.ModTime.UTC().Format(http.TimeFormat))
		}
		contentType := cacheMeta.Headers.Get("Content-Type")
		if contentType == "" {
			logging.Warn("Content-Type missing in cached metadata, detecting based on path.", "key", requestCacheKey, "path", relativePath)
			contentType = util.GetContentType(relativePath)
		}
		w.Header().Set("Content-Type", contentType)

		w.Header().Set("X-Cache-Status", "HIT")

		if r.Method == http.MethodHead {
			if cacheMeta.Size >= 0 {
				w.Header().Set("Content-Length", strconv.FormatInt(cacheMeta.Size, 10))
			}
			w.WriteHeader(http.StatusOK)
			logging.Debug("Served HEAD request from cache", "key", requestCacheKey)
			return
		}

		readSeeker, isSeeker := cacheReader.(io.ReadSeeker)
		if !isSeeker {
			logging.Error("Cache reader is not io.ReadSeeker, serving via io.Copy", "key", requestCacheKey)
			if cacheMeta.Size >= 0 {
				w.Header().Set("Content-Length", strconv.FormatInt(cacheMeta.Size, 10))
			}
			w.WriteHeader(http.StatusOK)
			bytesWritten, copyErr := io.Copy(w, cacheReader)
			if copyErr != nil && !isClientDisconnectedError(copyErr) {
				logging.ErrorE("Failed to write response body from non-seeker cache", copyErr, "key", requestCacheKey, "written_bytes", bytesWritten)
			} else if copyErr == nil {
				logging.Debug("Served bytes from non-seeker cache via io.Copy", "key", requestCacheKey, "written_bytes", bytesWritten)
			}
		} else {
			serveName := filepath.Base(relativePath)
			if serveName == "." || serveName == "/" {
				serveName = h.repoConfig.Name
			}
			logging.Debug("Serving cache hit with http.ServeContent", "key", requestCacheKey, "serve_name", serveName)
			http.ServeContent(w, r, serveName, cacheMeta.ModTime, readSeeker)
		}
		return
	}

	if !errors.Is(err, os.ErrNotExist) {
		logging.ErrorE("Error reading from cache", err, "key", requestCacheKey)
		http.Error(w, "Internal Cache Error", http.StatusInternalServerError)
		return
	}

	logging.Debug("Cache miss, fetching from upstream", "key", requestCacheKey, "upstream_url", upstreamURL)
	w.Header().Set("X-Cache-Status", "MISS")

	fetchResult, err := h.fetcher.Fetch(r.Context(), requestCacheKey, upstreamURL, r.Header)

	if err != nil {
		logFields := map[string]interface{}{"url": upstreamURL, "key": requestCacheKey, "repo": h.repoConfig.Name}
		if errors.Is(err, fetch.ErrNotFound) {
			logging.Warn("Fetch failed: Upstream resource not found (404)", logFields, "error", err)
			http.NotFound(w, r)
		} else if errors.Is(err, fetch.ErrUpstreamNotModified) {
			logging.Info("Fetch: Upstream returned 304 Not Modified", logFields)
			h.cacheManager.PutValidation(requestCacheKey, time.Now())
			w.WriteHeader(http.StatusNotModified)
		} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			logging.Warn("Fetch failed: Request context cancelled or deadline exceeded", logFields, "error", err)
		} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			logging.ErrorE("Fetch failed: Upstream request timeout", err, logFields)
			http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
		} else {
			logging.ErrorE("Fetch failed: Bad gateway or upstream connection error", err, logFields)
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		}
		return
	}

	defer fetchResult.Body.Close()

	finalCacheKey := requestCacheKey
	cachePutMeta := cache.CacheMetadata{
		Version:   cache.MetadataVersion,
		FetchTime: time.Now().UTC(),
		ModTime:   fetchResult.ModTime,
		Size:      fetchResult.Size,
		Headers:   make(http.Header),
	}
	util.SelectCacheHeaders(cachePutMeta.Headers, fetchResult.Header)

	finalContentType := ""
	upstreamContentType := fetchResult.Header.Get("Content-Type")
	if upstreamContentType != "" && !strings.HasPrefix(strings.ToLower(upstreamContentType), "application/octet-stream") {
		finalContentType = upstreamContentType
	} else {
		finalContentType = util.GetContentType(relativePath)
		logging.Debug("Using path-detected Content-Type", "path", relativePath, "detected_type", finalContentType, "upstream_type", upstreamContentType)
	}
	cachePutMeta.Headers.Set("Content-Type", finalContentType)
	w.Header().Set("Content-Type", finalContentType)

	pr, pw := io.Pipe()

	cacheErrChan := make(chan error, 1)
	cacheWriteDone := make(chan struct{})

	go func() {
		var putErr error
		defer func() {
			logging.Debug("Cache write goroutine sending result", "key", finalCacheKey, "error", putErr)
			cacheErrChan <- putErr
			close(cacheWriteDone)
		}()

		logging.Debug("Cache write goroutine started", "key", finalCacheKey)
		cacheWriteStart := time.Now()

		putErr = h.cacheManager.Put(context.Background(), finalCacheKey, pr, cachePutMeta)

		cacheWriteDuration := time.Since(cacheWriteStart)
		if putErr != nil {
			logging.ErrorE("Cache write goroutine finished with error", putErr, "key", finalCacheKey, "duration", util.FormatDuration(cacheWriteDuration))
			_ = pr.CloseWithError(putErr)
		} else {
			logging.Debug("Cache write goroutine finished successfully", "key", finalCacheKey, "duration", util.FormatDuration(cacheWriteDuration))
			h.cacheManager.PutValidation(finalCacheKey, time.Now())
		}
	}()

	util.ApplyCacheHeaders(w.Header(), fetchResult.Header)
	if !fetchResult.ModTime.IsZero() {
		w.Header().Set("Last-Modified", fetchResult.ModTime.UTC().Format(http.TimeFormat))
	}
	isChunked := len(fetchResult.Header.Values("Transfer-Encoding")) > 0 && fetchResult.Header.Get("Transfer-Encoding") != "identity"
	if fetchResult.Size >= 0 && !isChunked && fetchResult.StatusCode == http.StatusOK {
		w.Header().Set("Content-Length", strconv.FormatInt(fetchResult.Size, 10))
	}

	w.WriteHeader(fetchResult.StatusCode)

	if r.Method == http.MethodHead {
		logging.Debug("Handling HEAD request for cache miss, closing pipe writer", "key", finalCacheKey)
		if err := pw.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			logging.Warn("Error closing pipe writer for HEAD request", "error", err, "key", finalCacheKey)
		}
	} else {
		logging.Debug("Starting TeeReader copy to client and cache pipe", "key", finalCacheKey)
		teeReader := io.TeeReader(fetchResult.Body, pw)

		bytesWritten, copyErr := io.Copy(w, teeReader)

		logging.Debug("Finished TeeReader copy to client", "key", finalCacheKey, "bytes_written", bytesWritten, "copy_error", copyErr)
		if err := pw.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			logging.Warn("Error closing pipe writer after TeeReader copy", "error", err, "key", finalCacheKey)
		}

		if copyErr != nil && !isClientDisconnectedError(copyErr) {
			logging.Warn("Error copying response body to client", "error", copyErr, "key", finalCacheKey, "written_bytes", bytesWritten)
		} else if copyErr == nil {
			logging.Debug("Successfully served bytes from upstream via TeeReader", "key", finalCacheKey, "written_bytes", bytesWritten)
		}
	}

	logging.Debug("Waiting for cache write goroutine to finish", "key", finalCacheKey)
	cacheWriteErr := <-cacheErrChan
	<-cacheWriteDone

	if cacheWriteErr != nil {
		logging.Warn("Cache write for key finished with error (see previous log)", "key", finalCacheKey, "error", cacheWriteErr)
	} else {
		logging.Debug("Cache write confirmation received successfully", "key", finalCacheKey)
	}
}

func (h *RepositoryHandler) checkClientCacheHeaders(w http.ResponseWriter, r *http.Request, modTime time.Time, etag string) bool {
	clientETag := r.Header.Get("If-None-Match")
	if clientETag != "" && etag != "" {
		if strings.Contains(clientETag, etag) {
			logging.Debug("Cache check: ETag match", "client_if_none_match", clientETag, "cache_etag", etag, "path", r.URL.Path)
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	}

	clientModSince := r.Header.Get("If-Modified-Since")
	if clientModSince != "" && !modTime.IsZero() {
		if t, err := http.ParseTime(clientModSince); err == nil {
			if !modTime.Truncate(time.Second).After(t.Truncate(time.Second)) {
				logging.Debug("Cache check: Not modified since", "client_if_modified_since", clientModSince, "path", r.URL.Path, "cache_mod_time", modTime.UTC().Format(http.TimeFormat))
				w.WriteHeader(http.StatusNotModified)
				return true
			}
		} else {
			logging.Warn("Could not parse If-Modified-Since header", "error", err, "if_modified_since_header", clientModSince, "path", r.URL.Path)
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
		strings.Contains(errStr, "stream canceled")
}
