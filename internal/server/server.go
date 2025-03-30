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

const dirIndexKeySuffix = "/."

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

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		isRepoRequest := false
		if r.URL.Path != "/" {
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			if len(parts) > 0 && parts[0] != "" {
				repoName := parts[0]
				for _, repo := range cfg.Repositories {
					if repo.Name == repoName && repo.Enabled {
						isRepoRequest = true
						break
					}
				}

			}
		}

		if !isRepoRequest {

			if r.URL.Path == "/" {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				cacheStats := cacheManager.Stats()

				logging.Debug("Serving status page", "path", r.URL.Path)

				fmt.Fprint(w, `<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Go APT Proxy Status</title>
  <style>
    body { font-family: sans-serif; line-height: 1.6; color: #333; max-width: 900px; margin: 20px auto; padding: 0 15px; background-color: #f9f9f9; }
    .container { background-color: #fff; padding: 25px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
    h1, h2 { color: #2c3e50; border-bottom: 2px solid #ecf0f1; padding-bottom: 10px; margin-bottom: 20px; }
    h1 { font-size: 2em; } h2 { font-size: 1.5em; margin-top: 30px;}
    .repo-card { border: 1px solid #e0e0e0; border-radius: 5px; padding: 15px 20px; margin-bottom: 15px; background-color: #fff; transition: box-shadow 0.2s ease-in-out; }
    .repo-card:hover { box-shadow: 0 1px 5px rgba(0,0,0,0.1); }
    .repo-card h3 { margin-top: 0; margin-bottom: 10px; color: #3498db; font-size: 1.2em; }
    .repo-card p { margin: 5px 0; }
    .repo-card code { background-color: #ecf0f1; padding: 2px 6px; border-radius: 3px; font-family: monospace; font-size: 0.9em; word-break: break-all; }
    .status-box { margin-top: 30px; padding: 20px; background-color: #f8f9fa; border: 1px solid #e9ecef; border-radius: 5px; }
    .status-box h2 { border: none; margin-bottom: 15px;}
    .status-list { list-style: none; padding: 0; }
    .status-list li { margin-bottom: 8px; display: flex; justify-content: space-between; flex-wrap: wrap; }
    .status-list strong { color: #555; min-width: 180px; display: inline-block;}
    a { color: #3498db; text-decoration: none; } a:hover { text-decoration: underline; }
    .disabled-repo { opacity: 0.6; font-style: italic; }
    .enabled-status { color: #2ecc71; font-weight: bold;} .disabled-status { color: #e74c3c; font-weight: bold;}
    .boolean-true { color: #2ecc71; } .boolean-false { color: #e74c3c; }
  </style>
</head>
<body><div class="container"><h1>Go APT Proxy Status</h1><h2>Активные Репозитории</h2><div class="repos-list">`)

				hasEnabledRepos := false
				for _, repo := range cfg.Repositories {
					if !repo.Enabled {
						continue
					}
					hasEnabledRepos = true

					fmt.Fprintf(w, `<div class="repo-card"><h3>%s</h3><p><strong>Upstream URL:</strong> <a href="%s" target="_blank" rel="noopener noreferrer">%s</a></p><p><strong>Локальный URL:</strong> <a href="/%s/">/%s/</a></p><p><strong>Sources.list (пример):</strong><br><code id="aptUrl-%s">Загрузка...</code></p></div><script>(function(){var u=window.location.protocol+'//'+window.location.host;var n='%s';var i='aptUrl-%s';var e=document.getElementById(i);if(e){var d='stable';if(n.includes('ubuntu'))d='focal';  else if(n.includes('debian'))d='bookworm'; e.textContent='deb '+u+'/'+n+'/ '+d+' main';}})();</script>`,
						repo.Name, repo.URL, repo.URL, repo.Name, repo.Name, repo.Name, repo.Name, repo.Name)
				}
				if !hasEnabledRepos {
					fmt.Fprint(w, "<p>Нет активных репозиториев.</p>")
				}

				fmt.Fprintf(w, `</div><div class="status-box"><h2>Статус Кеша</h2><ul class="status-list"><li><strong>Статус кеша:</strong> <span class="%s">%s</span></li><li><strong>Директория кеша:</strong> <code>%s</code></li><li><strong>Кешировано файлов:</strong> %d</li><li><strong>Размер кеша:</strong> %s / %s</li><li><strong>Статус TTL валидации:</strong> <span class="%s">%s</span></li><li><strong>TTL валидации:</strong> %s</li><li><strong>Записей валидации:</strong> %d</li></ul><p><a href="/status">Подробный текстовый статус</a></p></div></div></body></html>`,
					boolToClass(cacheStats.CacheEnabled), boolToString(cacheStats.CacheEnabled),
					cacheStats.CacheDirectory, cacheStats.ItemCount,
					util.FormatSize(cacheStats.CurrentSize), util.FormatSize(cacheStats.MaxSize),
					boolToClass(cacheStats.ValidationTTLEnabled), boolToString(cacheStats.ValidationTTLEnabled),
					cacheStats.ValidationTTL.String(), cacheStats.ValidationItemCount)

				return
			} else {

				logging.Debug("Root handler: Path does not match status page or any repository prefix", "path", r.URL.Path)
				http.NotFound(w, r)
				return
			}
		}

	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
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
		fmt.Fprintf(w, "Cache Directory:        %s\n", cacheStats.CacheDirectory)
		fmt.Fprintf(w, "Cached Items:           %d\n", cacheStats.ItemCount)
		fmt.Fprintf(w, "Current Cache Size:     %s (%d bytes)\n", util.FormatSize(cacheStats.CurrentSize), cacheStats.CurrentSize)
		fmt.Fprintf(w, "Max Cache Size:         %s (%d bytes)\n", util.FormatSize(cacheStats.MaxSize), cacheStats.MaxSize)
		fmt.Fprintf(w, "Validation TTL Enabled: %t\n", cacheStats.ValidationTTLEnabled)
		fmt.Fprintf(w, "Validation TTL:         %s\n", cacheStats.ValidationTTL)
		fmt.Fprintf(w, "Validation Entries:     %d\n\n", cacheStats.ValidationItemCount)

		fmt.Fprintln(w, "--- Configured Repositories ---")
		for _, repo := range cfg.Repositories {
			status := "Disabled"
			if repo.Enabled {
				status = "Enabled"
			}
			fmt.Fprintf(w, "Name: %-15s Status: %-8s Upstream: %s\n", repo.Name, status, repo.URL)
		}

	})

	for _, repo := range cfg.Repositories {
		if !repo.Enabled {
			logging.Info("Skipping disabled repository", "repository_name", repo.Name)
			continue
		}

		pathPrefix := "/" + strings.Trim(repo.Name, "/") + "/"
		repoHandler := NewRepositoryHandler(repo, cfg.Server, cacheManager, fetcher)

		handlerPath := "/" + strings.Trim(repo.Name, "/")
		mux.Handle(pathPrefix, http.StripPrefix(handlerPath, repoHandler))

		logging.Info("Registered handler for repository", "repository_name", repo.Name, "path_prefix", pathPrefix, "upstream_url", repo.URL)
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
		logging.Warn("Method not allowed for repository handler", "method", r.Method, "repo", h.repoConfig.Name, "path", r.URL.Path)
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	relativePath := util.CleanPath(strings.TrimPrefix(r.URL.Path, "/"))

	if strings.HasPrefix(relativePath, "..") || strings.Contains(relativePath, "../") || strings.Contains(relativePath, "/..") {
		logging.Warn("Potentially malicious path detected after strip/clean", "cleaned_relative_path", relativePath, "original_request_uri", r.RequestURI, "repo", h.repoConfig.Name)
		http.Error(w, "Bad Request: Invalid Path", http.StatusBadRequest)
		return
	}

	originalPathEndsWithSlash := strings.HasSuffix(r.RequestURI, "/")

	isDirRequest := originalPathEndsWithSlash || relativePath == "." || relativePath == ""

	if relativePath == "" {
		relativePath = "."
	}

	baseCacheKey := filepath.ToSlash(filepath.Join(h.repoConfig.Name, relativePath))

	requestCacheKey := baseCacheKey
	if isDirRequest {

		if relativePath == "." {
			requestCacheKey = filepath.ToSlash(filepath.Join(h.repoConfig.Name, dirIndexKeySuffix))
		} else if !strings.HasSuffix(baseCacheKey, dirIndexKeySuffix) {

			requestCacheKey = baseCacheKey + dirIndexKeySuffix
		}

	}

	upstreamPath := strings.TrimPrefix(r.URL.Path, "/")
	upstreamURL := strings.TrimSuffix(h.repoConfig.URL, "/") + "/" + upstreamPath

	logging.Debug("Handling repository request",
		"repo", h.repoConfig.Name,
		"relative_path", relativePath,
		"original_uri_path", r.URL.Path,
		"upstream_path_part", upstreamPath,
		"base_cache_key", baseCacheKey,
		"request_cache_key", requestCacheKey,
		"upstream_url", upstreamURL,
		"is_dir_request", isDirRequest,
		"method", r.Method)

	if validationTime, ok := h.cacheManager.GetValidation(requestCacheKey); ok {

		logging.Debug("Validation cache hit, checking client cache headers", "key", requestCacheKey, "validated_at", validationTime.Format(time.RFC3339))

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

		if cacheMeta.Size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(cacheMeta.Size, 10))
		}

		contentType := cacheMeta.Headers.Get("Content-Type")
		if contentType == "" {
			logging.Warn("Content-Type missing in cached metadata, detecting based on path.", "key", requestCacheKey, "path", relativePath)
			contentType = util.GetContentType(relativePath)
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("X-Cache-Status", "HIT")

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}

		readSeeker, isSeeker := cacheReader.(io.ReadSeeker)
		if isSeeker {
			logging.Debug("Serving cache hit with http.ServeContent", "key", requestCacheKey)
			serveName := filepath.Base(relativePath)

			if isDirRequest || relativePath == "." {
				serveName = ""
			}
			http.ServeContent(w, r, serveName, cacheMeta.ModTime, readSeeker)
		} else {

			logging.Warn("Cache reader is not io.ReadSeeker, serving via io.Copy", "key", requestCacheKey)
			w.WriteHeader(http.StatusOK)
			bytesWritten, copyErr := io.Copy(w, cacheReader)
			if copyErr != nil && !isClientDisconnectedError(copyErr) {

				logging.ErrorE("Failed to write response body from non-seeker cache", copyErr, "key", requestCacheKey, "written_bytes", bytesWritten)
			} else if copyErr == nil {
				logging.Debug("Served bytes from non-seeker cache", "key", requestCacheKey, "written_bytes", bytesWritten)
			}
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

		logFields := map[string]interface{}{"url": upstreamURL, "key": requestCacheKey}
		if errors.Is(err, fetch.ErrNotFound) {
			logging.Warn("Fetch failed: Upstream resource not found (404)", logFields, "error", err)
			http.NotFound(w, r)
		} else if errors.Is(err, fetch.ErrUpstreamNotModified) {
			logging.Info("Fetch: Upstream returned 304 Not Modified", logFields)

			h.cacheManager.PutValidation(requestCacheKey, time.Now())
			w.WriteHeader(http.StatusNotModified)
		} else if errors.Is(err, context.Canceled) {
			logging.Warn("Fetch failed: Request canceled by client", logFields, "error", err)

		} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
			logging.ErrorE("Fetch failed: Upstream request timeout", err, logFields)
			http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
		} else {

			logging.ErrorE("Fetch failed: Bad gateway", err, logFields)
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		}
		return
	}

	defer fetchResult.Body.Close()

	upstreamContentType := fetchResult.Header.Get("Content-Type")
	isFetchedContentDirIndex := strings.Contains(strings.ToLower(upstreamContentType), "text/html")

	finalCacheKey := requestCacheKey
	if isFetchedContentDirIndex && !isDirRequest {

		finalCacheKey = baseCacheKey
		if relativePath != "." && !strings.HasSuffix(finalCacheKey, dirIndexKeySuffix) {
			finalCacheKey += dirIndexKeySuffix
		} else if relativePath == "." {
			finalCacheKey = filepath.ToSlash(filepath.Join(h.repoConfig.Name, dirIndexKeySuffix))
		}
		logging.Debug("Adjusted final cache key based on fetched Content-Type (HTML)", "original_request_key", requestCacheKey, "final_cache_key", finalCacheKey)
	} else if !isFetchedContentDirIndex && isDirRequest {

		finalCacheKey = baseCacheKey
		logging.Warn("Request key indicated directory, but fetched Content-Type was not HTML. Saving with non-directory key.", "request_key", requestCacheKey, "fetched_content_type", upstreamContentType, "final_cache_key", finalCacheKey)
	}

	cachePutMeta := cache.CacheMetadata{
		Version:   cache.MetadataVersion,
		FetchTime: time.Now().UTC(),
		ModTime:   fetchResult.ModTime,
		Size:      fetchResult.Size,
		Headers:   make(http.Header),
	}

	util.SelectCacheHeaders(cachePutMeta.Headers, fetchResult.Header)

	finalContentType := ""
	if isFetchedContentDirIndex {

		finalContentType = "text/html; charset=utf-8"
	} else if upstreamContentType != "" && !strings.HasPrefix(strings.ToLower(upstreamContentType), "application/octet-stream") {

		finalContentType = upstreamContentType
	} else {

		finalContentType = util.GetContentType(relativePath)
		logging.Debug("Determined Content-Type based on path extension", "path", relativePath, "content_type", finalContentType)
	}

	cachePutMeta.Headers.Set("Content-Type", finalContentType)

	pr, pw := io.Pipe()

	cacheErrChan := make(chan error, 1)
	go func() {
		defer close(cacheErrChan)

		cacheWriteStart := time.Now()
		putErr := h.cacheManager.Put(context.Background(), finalCacheKey, pr, cachePutMeta)
		cacheWriteDuration := time.Since(cacheWriteStart)

		if putErr != nil {
			logging.ErrorE("Cache write goroutine finished with error", putErr, "key", finalCacheKey, "duration", util.FormatDuration(cacheWriteDuration))

			_ = pr.CloseWithError(putErr)
		} else {
			logging.Debug("Cache write goroutine finished successfully", "key", finalCacheKey, "duration", util.FormatDuration(cacheWriteDuration))

			h.cacheManager.PutValidation(finalCacheKey, time.Now())
		}
		cacheErrChan <- putErr
	}()

	util.ApplyCacheHeaders(w.Header(), fetchResult.Header)

	if !fetchResult.ModTime.IsZero() {
		w.Header().Set("Last-Modified", fetchResult.ModTime.UTC().Format(http.TimeFormat))
	}

	if fetchResult.Size >= 0 && fetchResult.Header.Get("Transfer-Encoding") == "" && fetchResult.StatusCode != http.StatusPartialContent {
		w.Header().Set("Content-Length", strconv.FormatInt(fetchResult.Size, 10))
	}

	w.Header().Set("Content-Type", finalContentType)

	w.WriteHeader(fetchResult.StatusCode)

	if r.Method == http.MethodHead {

		logging.Debug("Handling HEAD request, closing pipe writer without copy", "key", finalCacheKey)

		if err := pw.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			logging.Warn("Error closing pipe writer for HEAD request", "error", err, "key", finalCacheKey)
		}
	} else {

		teeReader := io.TeeReader(fetchResult.Body, pw)
		bytesWritten, copyErr := io.Copy(w, teeReader)

		pipeCloseErr := pw.Close()
		if pipeCloseErr != nil && !errors.Is(pipeCloseErr, io.ErrClosedPipe) {
			logging.Warn("Error closing pipe writer after copy", "error", pipeCloseErr, "key", finalCacheKey)
		}

		if copyErr != nil && !isClientDisconnectedError(copyErr) {

			logging.Warn("Error copying response body to client", "error", copyErr, "key", finalCacheKey, "written_bytes", bytesWritten)

			_ = pr.CloseWithError(copyErr)
		} else if copyErr == nil {

			logging.Debug("Served bytes from upstream", "key", finalCacheKey, "written_bytes", bytesWritten)
		}

	}

	select {
	case cacheWriteErr := <-cacheErrChan:
		if cacheWriteErr != nil {

			logging.Warn("Cache write for key finished with error (see previous log)", "key", finalCacheKey)
		} else {

			logging.Debug("Cache write confirmation received", "key", finalCacheKey)
		}
	case <-time.After(30 * time.Second):
		logging.Warn("Cache write did not complete within confirmation timeout after response sent/aborted.", "key", finalCacheKey)

		_ = pr.CloseWithError(errors.New("cache write timeout after response sent"))
	}
}

func (h *RepositoryHandler) checkClientCacheHeaders(w http.ResponseWriter, r *http.Request, modTime time.Time, etag string) bool {

	clientETag := r.Header.Get("If-None-Match")
	if clientETag != "" && etag != "" {

		if strings.Contains(clientETag, etag) {
			logging.Debug("Cache check: ETag match", "client_etag", clientETag, "cache_etag", etag, "path", r.URL.Path)
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
			logging.Warn("Could not parse If-Modified-Since header", "error", err, "if_modified_since_header", clientModSince)
		}
	}

	return false
}

func isClientDisconnectedError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}

	if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) {
		return true
	}

	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "client disconnected") ||
		strings.Contains(errStr, "request canceled") ||
		strings.Contains(errStr, "connection closed by client") ||
		strings.Contains(errStr, "tls: user canceled") ||
		strings.Contains(errStr, "connection closed") ||
		strings.Contains(errStr, "write: broken pipe")
}

func boolToString(b bool) string {
	if b {
		return "Enabled"
	}
	return "Disabled"
}

func boolToClass(b bool) string {
	if b {
		return "boolean-true"
	}
	return "boolean-false"
}
