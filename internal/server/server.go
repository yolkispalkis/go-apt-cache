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
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			host := r.Host
			fmt.Fprintf(w, `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Go APT Proxy</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
    h1 { color: #333; }
    .repo { margin-bottom: 20px; border: 1px solid #ddd; padding: 15px; border-radius: 5px; }
    .repo h2 { margin-top: 0; color: #0066cc; }
    .status { margin-top: 30px; padding: 10px; background-color: #f8f8f8; border-radius: 5px; }
  </style>
</head>
<body>
  <h1>Go APT Proxy</h1>
  <p>Активные репозитории:</p>
  <div class="repos">`)

			for _, repo := range cfg.Repositories {
				if !repo.Enabled {
					continue
				}

				fmt.Fprintf(w, `
    <div class="repo">
      <h2>%s</h2>
      <p>Upstream URL: <a href="%s">%s</a></p>
      <p>Локальный URL: <a href="/%s/">/%s/</a></p>
      <p>Строка для sources.list: <code id="aptUrl-%s">deb http:
      <script>
        document.addEventListener('DOMContentLoaded', function() {
          var baseUrl = window.location.protocol + '//' + window.location.host;
          var repoName = '%s';
          var aptUrlId = 'aptUrl-%s';
          var aptUrl = document.getElementById(aptUrlId);
          if (aptUrl) {
            aptUrl.textContent = 'deb ' + baseUrl + '/' + repoName + '/ release main';
          }
        });
      </script>
    </div>`, repo.Name, repo.URL, repo.URL, repo.Name, repo.Name, repo.Name, host, repo.Name)
			}

			cacheStats := cacheManager.Stats()
			fmt.Fprintf(w, `
  </div>
  <div class="status">
    <h3>Статус кеша:</h3>
    <p>Количество элементов: %d</p>
    <p>Размер кеша: %s / %s</p>
    <p><a href="/status">Подробная информация о статусе</a></p>
  </div>
</body>
</html>`, cacheStats.ItemCount, util.FormatSize(cacheStats.CurrentSize), util.FormatSize(cacheStats.MaxSize))
			return
		}

		if r.URL.Path != "/status" && !strings.HasPrefix(r.URL.Path, "/") {
			http.NotFound(w, r)
			return
		}
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, "OK")
		cacheStats := cacheManager.Stats()
		fmt.Fprintf(w, "Cache Items: %d\n", cacheStats.ItemCount)
		fmt.Fprintf(w, "Cache Size: %s / %s\n", util.FormatSize(cacheStats.CurrentSize), util.FormatSize(cacheStats.MaxSize))
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

	filePath := strings.TrimPrefix(r.URL.Path, "/")

	cacheKey := h.repoConfig.Name + "/" + filePath

	upstreamURL := strings.TrimSuffix(h.repoConfig.URL, "/") + "/" + filePath

	validationTime, ok := h.cacheManager.GetValidation(cacheKey)
	if ok {
		if h.checkClientCacheHeaders(w, r, validationTime) {
			return
		}
	}

	cacheReader, cacheSize, cacheModTime, err := h.cacheManager.Get(r.Context(), cacheKey)
	if err == nil {
		defer cacheReader.Close()
		logging.Debug("Cache hit for key: %s", cacheKey)

		if h.checkClientCacheHeaders(w, r, cacheModTime) {
			return
		}

		w.Header().Set("Last-Modified", cacheModTime.UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", cacheSize))

		w.Header().Set("Content-Type", util.GetContentType(filePath))

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}

		readSeeker, ok := cacheReader.(io.ReadSeeker)
		if ok {

			http.ServeContent(w, r, filePath, cacheModTime, readSeeker)
		} else {

			logging.Warn("Cache reader for %s is not a ReadSeeker, serving via io.Copy", cacheKey)
			w.WriteHeader(http.StatusOK)
			_, copyErr := io.Copy(w, cacheReader)
			if copyErr != nil && !isClientDisconnectedError(copyErr) {
				logging.ErrorE("Failed to write response body from cache", copyErr, "key", cacheKey)
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

	fetchResult, err := h.fetcher.Fetch(r.Context(), cacheKey, upstreamURL, r.Header)
	if err != nil {
		logging.Error("Failed to fetch %s (key %s) from upstream: %v", upstreamURL, cacheKey, err)
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
		if err != nil {
			logging.Error("Failed to write key %s to cache: %v", cacheKey, err)
		}
		cacheErrChan <- err
	}()

	teeReader := io.TeeReader(fetchResult.Body, pw)

	w.WriteHeader(fetchResult.StatusCode)

	if r.Method == http.MethodHead {
		pw.Close()
		select {
		case <-cacheErrChan:
		case <-time.After(100 * time.Millisecond):
		}
		return
	}

	_, copyErr := io.Copy(w, teeReader)

	pipeCloseErr := pw.Close()
	if pipeCloseErr != nil && !errors.Is(pipeCloseErr, io.ErrClosedPipe) {
		logging.Warn("Error closing pipe writer for %s: %v", cacheKey, pipeCloseErr)
	}

	if copyErr != nil && !isClientDisconnectedError(copyErr) {
		logging.Warn("Error copying response to client for %s: %v", cacheKey, copyErr)
	}

	select {
	case cacheWriteErr := <-cacheErrChan:
		if cacheWriteErr != nil {
			logging.Warn("Cache write finished with error for %s", cacheKey)
		} else {
			logging.Debug("Cache write finished successfully for %s", cacheKey)
		}
	case <-time.After(30 * time.Second):
		logging.Warn("Cache write for %s did not complete within timeout", cacheKey)
	}
}

func (h *RepositoryHandler) checkClientCacheHeaders(w http.ResponseWriter, r *http.Request, modTime time.Time) bool {
	if modTime.IsZero() {
		return false
	}

	ifims := r.Header.Get("If-Modified-Since")
	if ifims != "" {
		if t, err := http.ParseTime(ifims); err == nil {
			modTimeTruncated := modTime.Truncate(time.Second)
			clientTimeTruncated := t.Truncate(time.Second)
			if !modTimeTruncated.After(clientTimeTruncated) {
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
	errStr := err.Error()
	if strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "client disconnected") ||
		strings.Contains(errStr, "tls: user canceled") {
		return true
	}
	return false
}
