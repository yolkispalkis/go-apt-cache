package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
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

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		if r.URL.Path != "/" {

			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			if len(parts) > 0 {
				repoName := parts[0]
				for _, repo := range cfg.Repositories {
					if repo.Enabled && repo.Name == repoName {

						http.NotFound(w, r)
						return
					}
				}
			}

			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		cacheStats := cacheManager.Stats()

		fmt.Fprint(w, `<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Go APT Proxy Status</title>
  <style>
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen-Sans, Ubuntu, Cantarell, "Helvetica Neue", sans-serif;
      line-height: 1.6;
      color: #333;
      max-width: 900px;
      margin: 20px auto;
      padding: 0 15px;
      background-color: #f9f9f9;
    }
    .container {
      background-color: #fff;
      padding: 25px;
      border-radius: 8px;
      box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }
    h1, h2 {
      color: #2c3e50;
      border-bottom: 2px solid #ecf0f1;
      padding-bottom: 10px;
      margin-bottom: 20px;
    }
    h1 { font-size: 2em; }
    h2 { font-size: 1.5em; margin-top: 30px;}
    .repo-card {
      border: 1px solid #e0e0e0;
      border-radius: 5px;
      padding: 15px 20px;
      margin-bottom: 15px;
      background-color: #fff;
      transition: box-shadow 0.2s ease-in-out;
    }
    .repo-card:hover {
        box-shadow: 0 1px 5px rgba(0,0,0,0.1);
    }
    .repo-card h3 {
      margin-top: 0;
      margin-bottom: 10px;
      color: #3498db;
      font-size: 1.2em;
    }
    .repo-card p { margin: 5px 0; }
    .repo-card code {
      background-color: #ecf0f1;
      padding: 2px 6px;
      border-radius: 3px;
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, Courier, monospace;
      font-size: 0.9em;
      word-break: break-all;
    }
    .status-box {
      margin-top: 30px;
      padding: 20px;
      background-color: #f8f9fa;
      border: 1px solid #e9ecef;
      border-radius: 5px;
    }
    .status-box h2 { border: none; margin-bottom: 15px;}
    .status-list { list-style: none; padding: 0; }
    .status-list li { margin-bottom: 8px; display: flex; justify-content: space-between; }
    .status-list strong { color: #555; min-width: 180px; display: inline-block;}
    a { color: #3498db; text-decoration: none; }
    a:hover { text-decoration: underline; }
    .disabled-repo { opacity: 0.6; font-style: italic; }
    .enabled-status { color: #2ecc71; font-weight: bold;}
    .disabled-status { color: #e74c3c; font-weight: bold;}
    .boolean-true { color: #2ecc71; }
    .boolean-false { color: #e74c3c; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Go APT Proxy Status</h1>

    <h2>Активные Репозитории</h2>
    <div class="repos-list">`)

		hasEnabledRepos := false
		for _, repo := range cfg.Repositories {
			if !repo.Enabled {
				continue
			}
			hasEnabledRepos = true
			fmt.Fprintf(w, `
      <div class="repo-card">
        <h3>%s</h3>
        <p><strong>Upstream URL:</strong> <a href="%s" target="_blank" rel="noopener noreferrer">%s</a></p>
        <p><strong>Локальный URL:</strong> <a href="/%s/">/%s/</a></p>
        <p><strong>Sources.list (пример):</strong><br><code id="aptUrl-%s">Загрузка...</code></p>
      </div>
      <script>
        (function() {
          var baseUrl = window.location.protocol + '//' + window.location.host;
          var repoName = '%s';
          var aptUrlId = 'aptUrl-%s';
          var aptUrlElement = document.getElementById(aptUrlId);
          if (aptUrlElement) {

            var distribution = 'stable'; 
            if (repoName.includes('ubuntu')) distribution = 'focal'; 
            aptUrlElement.textContent = 'deb ' + baseUrl + '/' + repoName + '/ ' + distribution + ' main';
          }
        })();
      </script>`, repo.Name, repo.URL, repo.URL, repo.Name, repo.Name, repo.Name, repo.Name, repo.Name)
		}

		if !hasEnabledRepos {
			fmt.Fprint(w, "<p>Нет активных репозиториев.</p>")
		}

		fmt.Fprintf(w, `
    </div>

    <div class="status-box">
      <h2>Статус Кеша</h2>
      <ul class="status-list">
        <li><strong>Статус Кеша:</strong> <span class="%s">%s</span></li>
        <li><strong>Директория Кеша:</strong> <code>%s</code></li>
        <li><strong>Кешировано Файлов:</strong> %d</li>
        <li><strong>Размер Кеша:</strong> %s / %s</li>
        <li><strong>Статус TTL Валидации:</strong> <span class="%s">%s</span></li>
        <li><strong>TTL Валидации:</strong> %s</li>
        <li><strong>Записей Валидации:</strong> %d</li>
      </ul>
      <p><a href="/status">Подробный текстовый статус</a></p>
    </div>
  </div>
</body>
</html>`,
			boolToClass(cacheStats.CacheEnabled), boolToString(cacheStats.CacheEnabled),
			cacheStats.CacheDirectory,
			cacheStats.ItemCount,
			util.FormatSize(cacheStats.CurrentSize), util.FormatSize(cacheStats.MaxSize),
			boolToClass(cacheStats.ValidationTTLEnabled), boolToString(cacheStats.ValidationTTLEnabled),
			cacheStats.ValidationTTL.String(),
			cacheStats.ValidationItemCount)
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		cacheStats := cacheManager.Stats()

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
			logging.Info("Skipping disabled repository: %s", repo.Name)
			continue
		}

		pathPrefix := "/" + strings.Trim(repo.Name, "/") + "/"
		repoHandler := NewRepositoryHandler(repo, cfg.Server, cacheManager, fetcher)

		mux.Handle(pathPrefix, http.StripPrefix(pathPrefix, repoHandler))
		logging.Info("Registered handler for repository %q at path %s (Upstream: %s)", repo.Name, pathPrefix, repo.URL)
	}

	var handler http.Handler = mux
	handler = LoggingMiddleware(handler)
	handler = RecoveryMiddleware(handler)

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
	cache cache.CacheManager,
	fetcher *fetch.Coordinator,
) *RepositoryHandler {
	return &RepositoryHandler{
		repoConfig:   repo,
		serverConfig: serverCfg,
		cacheManager: cache,
		fetcher:      fetcher,
	}
}

func (h *RepositoryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	filePath := util.CleanPath(r.URL.Path)
	if strings.HasPrefix(filePath, "..") || strings.HasPrefix(filePath, "/") {
		logging.Warn("Potentially malicious path detected: %s (original: %s)", filePath, r.URL.Path)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	cacheKey := h.repoConfig.Name + "/" + filePath

	upstreamURL := strings.TrimSuffix(h.repoConfig.URL, "/") + "/" + filePath

	logging.Debug("Handling request: Repo=%s, RelativePath=%s, CacheKey=%s, UpstreamURL=%s",
		h.repoConfig.Name, filePath, cacheKey, upstreamURL)

	validationTime, ok := h.cacheManager.GetValidation(cacheKey)
	if ok {

		if h.checkClientCacheHeaders(w, r, validationTime) {
			logging.Debug("Validation cache hit and client cache valid for %s", cacheKey)
			return
		}

		logging.Debug("Validation cache hit for %s, but client needs data or cache is stale", cacheKey)
	}

	cacheReader, cacheSize, cacheModTime, err := h.cacheManager.Get(r.Context(), cacheKey)
	if err == nil {

		defer cacheReader.Close()
		logging.Debug("Disk cache hit for key: %s", cacheKey)

		if h.checkClientCacheHeaders(w, r, cacheModTime) {
			return
		}

		w.Header().Set("Last-Modified", cacheModTime.UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", cacheSize))

		w.Header().Set("Content-Type", util.GetContentType(filePath))
		w.Header().Set("X-Cache-Status", "HIT")

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}

		readSeeker, isSeeker := cacheReader.(io.ReadSeeker)
		if isSeeker {

			http.ServeContent(w, r, filePath, cacheModTime, readSeeker)
		} else {

			logging.Warn("Cache reader for %s is not a ReadSeeker, serving via io.Copy", cacheKey)
			w.WriteHeader(http.StatusOK)
			_, copyErr := io.Copy(w, cacheReader)
			if copyErr != nil && !isClientDisconnectedError(copyErr) {
				logging.ErrorE("Failed to write response body from non-seeker cache", copyErr, "key", cacheKey)
			}
		}
		return
	}

	if !errors.Is(err, os.ErrNotExist) {
		logging.Error("Error reading from cache for key %s: %v", cacheKey, err)
		http.Error(w, "Internal Cache Error", http.StatusInternalServerError)
		return
	}

	logging.Debug("Cache miss for key: %s, fetching from upstream: %s", cacheKey, upstreamURL)
	w.Header().Set("X-Cache-Status", "MISS")

	fetchResult, err := h.fetcher.Fetch(r.Context(), cacheKey, upstreamURL, r.Header)
	if err != nil {
		logging.Warn("Failed to fetch %s (key %s) from upstream: %v", upstreamURL, cacheKey, err)
		if errors.Is(err, fetch.ErrNotFound) {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		} else if errors.Is(err, fetch.ErrUpstreamNotModified) {

			h.cacheManager.PutValidation(cacheKey, time.Now())
			w.WriteHeader(http.StatusNotModified)
		} else {

			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		}
		return
	}

	defer fetchResult.Body.Close()

	h.cacheManager.PutValidation(cacheKey, time.Now())

	util.CopyRelevantHeaders(w.Header(), fetchResult.Header)

	upstreamContentType := fetchResult.Header.Get("Content-Type")
	if upstreamContentType != "" && upstreamContentType != "application/octet-stream" {
		w.Header().Set("Content-Type", upstreamContentType)
	} else {

		w.Header().Set("Content-Type", util.GetContentType(filePath))
	}

	if _, ok := w.Header()["Last-Modified"]; !ok && !fetchResult.ModTime.IsZero() {
		w.Header().Set("Last-Modified", fetchResult.ModTime.UTC().Format(http.TimeFormat))
	}

	pr, pw := io.Pipe()

	cacheErrChan := make(chan error, 1)
	go func() {
		defer close(cacheErrChan)

		modTimeToUse := fetchResult.ModTime
		if modTimeToUse.IsZero() {

			modTimeToUse = time.Now()
		}

		err := h.cacheManager.Put(context.Background(), cacheKey, pr, fetchResult.Size, modTimeToUse)

		cacheErrChan <- err
		if err != nil {
			logging.Error("Failed to write key %s to cache: %v", cacheKey, err)

			_, _ = io.Copy(io.Discard, pr)
		}
	}()

	teeReader := io.TeeReader(fetchResult.Body, pw)

	w.WriteHeader(fetchResult.StatusCode)

	if r.Method == http.MethodHead {
		pw.Close()

		select {
		case cacheWriteErr := <-cacheErrChan:
			if cacheWriteErr != nil {
				logging.Warn("Cache write for HEAD request %s finished with error: %v", cacheKey, cacheWriteErr)
			}
		case <-time.After(200 * time.Millisecond):

		}
		return
	}

	bytesWritten, copyErr := io.Copy(w, teeReader)

	pipeCloseErr := pw.Close()
	if pipeCloseErr != nil && !errors.Is(pipeCloseErr, io.ErrClosedPipe) {
		logging.Warn("Error closing pipe writer for %s: %v", cacheKey, pipeCloseErr)
	}

	if copyErr != nil && !isClientDisconnectedError(copyErr) {

		logging.Warn("Error copying response to client for %s (%d bytes written): %v", cacheKey, bytesWritten, copyErr)
	}

	select {
	case cacheWriteErr := <-cacheErrChan:
		if cacheWriteErr != nil {

			logging.Warn("Cache write finished with error for %s", cacheKey)
		} else {
			logging.Debug("Cache write finished successfully for %s", cacheKey)
		}
	case <-time.After(30 * time.Second):
		logging.Warn("Cache write for %s did not complete within timeout after client copy finished", cacheKey)

		_ = pr.CloseWithError(errors.New("cache write timeout"))
	}
}

func (h *RepositoryHandler) checkClientCacheHeaders(w http.ResponseWriter, r *http.Request, modTime time.Time) bool {
	if modTime.IsZero() {
		return false
	}

	ifims := r.Header.Get("If-Modified-Since")
	if ifims != "" {
		if t, err := http.ParseTime(ifims); err == nil {

			if !modTime.Truncate(time.Second).After(t.Truncate(time.Second)) {
				w.WriteHeader(http.StatusNotModified)
				return true
			}
		} else {
			logging.Warn("Could not parse If-Modified-Since header '%s': %v", ifims, err)
		}
	}

	return false
}

func isClientDisconnectedError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.ErrClosedPipe) {
		return true
	}

	if errors.Is(err, context.Canceled) {
		return true
	}

	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "client disconnected") ||
		strings.Contains(errStr, "request canceled") ||
		strings.Contains(errStr, "connection closed by client") ||
		strings.Contains(errStr, "tls: user canceled") {
		return true
	}
	return false
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
