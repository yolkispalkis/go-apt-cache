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

	// Handler for the root path "/"
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
				// Script to dynamically update the host in the example sources.list line
				fmt.Fprintf(w, `
    <div class="repo">
      <h2>%s</h2>
      <p>Upstream URL: <a href="%s">%s</a></p>
      <p>Локальный URL: <a href="/%s/">/%s/</a></p>
      <p>Строка для sources.list: <code id="aptUrl-%s">deb http://%s/%s/ release main</code></p>
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
    </div>`, repo.Name, repo.URL, repo.URL, repo.Name, repo.Name, repo.Name, host, repo.Name, repo.Name, repo.Name)
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
		// Handle other root-level paths like /status or non-repository paths
		if r.URL.Path != "/status" && !strings.HasPrefix(r.URL.Path, "/") {
			http.NotFound(w, r)
			return
		}
	})

	// Handler for /status
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

	// Register handlers for each enabled repository
	for _, repo := range cfg.Repositories {
		if !repo.Enabled {
			logging.Info("Skipping disabled repository: %s", repo.Name)
			continue
		}

		// Ensure path prefix includes the trailing slash for StripPrefix
		pathPrefix := "/" + strings.Trim(repo.Name, "/") + "/"
		repoHandler := NewRepositoryHandler(repo, cfg.Server, cacheManager, fetcher)

		mux.Handle(pathPrefix, http.StripPrefix(pathPrefix, repoHandler))
		logging.Info("Registered handler for repository %q at path %s (Upstream: %s)", repo.Name, pathPrefix, repo.URL)
	}

	// Apply middleware
	var handler http.Handler = mux
	handler = LoggingMiddleware(handler)
	handler = RecoveryMiddleware(handler)

	// Configure the HTTP server
	httpServer := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.Duration(),
		IdleTimeout:       cfg.Server.IdleTimeout.Duration(),
		// Add other timeouts like WriteTimeout if needed
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

// ServeHTTP handles requests for a specific repository.
func (h *RepositoryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Only allow GET and HEAD requests
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	// filePath is relative to the repository root (e.g., "dists/stable/Release")
	filePath := strings.TrimPrefix(r.URL.Path, "/")
	// isDirRequest checks if the original request path ended with a slash
	isDirRequest := strings.HasSuffix(r.URL.Path, "/") || r.URL.Path == ""

	// Handle request for the repository root (e.g., /ubuntu/)
	if filePath == "" && isDirRequest {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		host := r.Host
		// Display a simple info page for the repository root
		fmt.Fprintf(w, `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>%s Repository - Go APT Proxy</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
    h1 { color: #333; }
    .repo-info { margin-bottom: 20px; border: 1px solid #ddd; padding: 15px; border-radius: 5px; }
    .repo-info h2 { margin-top: 0; color: #0066cc; }
    .note { margin-top: 30px; padding: 10px; background-color: #f8f8f8; border-radius: 5px; }
  </style>
</head>
<body>
  <h1>%s Repository</h1>
  <div class="repo-info">
    <h2>Repository Information</h2>
    <p>Upstream URL: <a href="%s">%s</a></p>
    <p>Local URL: <a href="/%s/">/%s/</a></p>
  </div>
  <div class="note">
    <p>Это прокси-кеш для репозитория APT. Используйте этот URL в вашем sources.list:</p>
    <pre id="aptUrl">deb http://%s/%s/ release main</pre>
    <script>
      document.addEventListener('DOMContentLoaded', function() {
        var baseUrl = window.location.protocol + '//' + window.location.host;
        var repoName = '%s';
        var aptUrl = document.getElementById('aptUrl');
        aptUrl.textContent = 'deb ' + baseUrl + '/' + repoName + '/ release main';
      });
    </script>
    <p>Замените "release" и "main" соответствующими значениями для вашего дистрибутива.</p>
  </div>
  <p><a href="/">← Вернуться на главную страницу</a></p>
</body>
</html>`, h.repoConfig.Name, h.repoConfig.Name, h.repoConfig.URL, h.repoConfig.URL, h.repoConfig.Name, h.repoConfig.Name, host, h.repoConfig.Name, h.repoConfig.Name)
		return
	}

	// Construct cache key and upstream URL
	cacheKey := h.repoConfig.Name + "/" + filePath
	upstreamURL := strings.TrimSuffix(h.repoConfig.URL, "/") + "/" + filePath

	// Check validation cache first (quick check for recent fetches)
	validationTime, ok := h.cacheManager.GetValidation(cacheKey)
	if ok {
		if h.checkClientCacheHeaders(w, r, validationTime) {
			return // Respond with 304 Not Modified based on validation cache time
		}
	}

	// --- Cache Hit Handling ---
	cacheReader, cacheSize, cacheModTime, err := h.cacheManager.Get(r.Context(), cacheKey)
	if err == nil {
		defer cacheReader.Close()
		logging.Debug("Cache hit for key: %s", cacheKey)

		if h.checkClientCacheHeaders(w, r, cacheModTime) {
			return // Respond with 304 Not Modified based on actual file modTime
		}

		// Set basic headers from cache metadata
		w.Header().Set("Last-Modified", cacheModTime.UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", cacheSize))
		// Determine Content-Type primarily by file extension
		w.Header().Set("Content-Type", util.GetContentType(filePath))

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Try to use http.ServeContent for Range request support if reader is seekable
		readSeeker, ok := cacheReader.(io.ReadSeeker)
		if ok {
			http.ServeContent(w, r, filePath, cacheModTime, readSeeker)
		} else {
			// Fallback if not seekable (less likely for file cache but possible)
			logging.Warn("Cache reader for %s is not a ReadSeeker, serving via io.Copy", cacheKey)
			w.WriteHeader(http.StatusOK) // Ensure status is set before writing body
			_, copyErr := io.Copy(w, cacheReader)
			if copyErr != nil && !isClientDisconnectedError(copyErr) {
				logging.ErrorE("Failed to write response body from cache", copyErr, "key", cacheKey)
			}
		}
		return // Done handling cache hit
	}

	// Handle errors other than "not found" during cache access
	if !errors.Is(err, os.ErrNotExist) {
		logging.Error("Error reading from cache for key %s: %v", cacheKey, err)
		http.Error(w, "Internal Cache Error", http.StatusInternalServerError)
		return
	}

	// --- Cache Miss Handling ---
	logging.Debug("Cache miss for key: %s, fetching from upstream: %s", cacheKey, upstreamURL)

	// Fetch from upstream, passing client's conditional headers
	fetchResult, err := h.fetcher.Fetch(r.Context(), cacheKey, upstreamURL, r.Header)
	if err != nil {
		logging.Error("Failed to fetch %s (key %s) from upstream: %v", upstreamURL, cacheKey, err)
		if errors.Is(err, fetch.ErrNotFound) {
			// If original request looked like a directory, don't try index.html here,
			// just return 404 for the original path. Upstream index pages aren't proxied directly.
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		} else if errors.Is(err, fetch.ErrUpstreamNotModified) {
			// Upstream says not modified based on *client's* If-Modified-Since.
			// Update our validation cache and return 304 to the client.
			h.cacheManager.PutValidation(cacheKey, time.Now())
			w.WriteHeader(http.StatusNotModified)
		} else {
			// Other upstream errors (5xx, timeout, etc.)
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		}
		return
	}
	defer fetchResult.Body.Close()

	// Mark validation time now that we have a successful fetch
	h.cacheManager.PutValidation(cacheKey, time.Now())

	// Copy relevant headers (Content-Length, ETag, Cache-Control etc.) from upstream response
	util.CopyRelevantHeaders(w.Header(), fetchResult.Header)

	// Determine Content-Type: prioritize specific upstream header, fallback to extension
	upstreamContentType := fetchResult.Header.Get("Content-Type")
	if upstreamContentType != "" && upstreamContentType != "application/octet-stream" {
		w.Header().Set("Content-Type", upstreamContentType)
	} else {
		// Fallback to extension-based detection if upstream type is missing or generic
		w.Header().Set("Content-Type", util.GetContentType(filePath))
	}
	// Ensure Last-Modified is set if upstream provided it (CopyRelevantHeaders should handle this)
	if _, ok := w.Header()["Last-Modified"]; !ok && !fetchResult.ModTime.IsZero() {
		w.Header().Set("Last-Modified", fetchResult.ModTime.UTC().Format(http.TimeFormat))
	}

	// Use Pipe and TeeReader to write to cache and client simultaneously
	pr, pw := io.Pipe()
	cacheErrChan := make(chan error, 1)

	// Goroutine to save the fetched content to cache via the pipe reader
	go func() {
		defer close(cacheErrChan)
		modTimeToUse := fetchResult.ModTime
		if modTimeToUse.IsZero() {
			// Use current time if upstream didn't provide Last-Modified
			// This affects the Last-Modified header on subsequent cache hits
			modTimeToUse = time.Now()
		}
		// Pass fetchResult.Size (-1 if unknown) to Put for potential size check
		err := h.cacheManager.Put(context.Background(), cacheKey, pr, fetchResult.Size, modTimeToUse)
		if err != nil {
			// Log error, but don't propagate to client response
			logging.Error("Failed to write key %s to cache: %v", cacheKey, err)
		}
		cacheErrChan <- err // Signal completion, even if error occurred
	}()

	// TeeReader reads from upstream (fetchResult.Body) and writes to the pipe writer (pw)
	teeReader := io.TeeReader(fetchResult.Body, pw)

	// Write headers and status code *before* writing body
	w.WriteHeader(fetchResult.StatusCode)

	// Skip body copy for HEAD requests
	if r.Method == http.MethodHead {
		pw.Close() // Close pipe writer, nothing to cache body
		// Wait briefly for caching goroutine (likely completes instantly as pipe is closed)
		select {
		case <-cacheErrChan:
		case <-time.After(100 * time.Millisecond):
		}
		return
	}

	// Copy response body from teeReader to the client (http.ResponseWriter)
	_, copyErr := io.Copy(w, teeReader)

	// Close the pipe writer *after* copying is done or failed
	// This signals EOF to the reader side (cacheManager.Put)
	pipeCloseErr := pw.Close() // Check error on close as well
	if pipeCloseErr != nil && !errors.Is(pipeCloseErr, io.ErrClosedPipe) {
		logging.Warn("Error closing pipe writer for %s: %v", cacheKey, pipeCloseErr)
	}

	// Handle client disconnection errors gracefully during copy
	if copyErr != nil && !isClientDisconnectedError(copyErr) {
		logging.Warn("Error copying response to client for %s: %v", cacheKey, copyErr)
	}

	// Wait for caching goroutine to finish
	select {
	case cacheWriteErr := <-cacheErrChan:
		if cacheWriteErr != nil {
			// Error already logged in the goroutine
			logging.Warn("Cache write finished with error for %s", cacheKey)
		} else {
			logging.Debug("Cache write finished successfully for %s", cacheKey)
		}
	case <-time.After(30 * time.Second): // Increased timeout for potentially large files
		logging.Warn("Cache write for %s did not complete within timeout", cacheKey)
		// Consider canceling the Put context if cacheManager supports it?
	}
}

// checkClientCacheHeaders checks If-Modified-Since and returns true if 304 should be sent.
func (h *RepositoryHandler) checkClientCacheHeaders(w http.ResponseWriter, r *http.Request, modTime time.Time) bool {
	if modTime.IsZero() {
		return false // Cannot validate without modTime
	}

	ifims := r.Header.Get("If-Modified-Since")
	if ifims != "" {
		if t, err := http.ParseTime(ifims); err == nil {
			// Truncate to second precision for comparison as per RFC 7232
			modTimeTruncated := modTime.Truncate(time.Second)
			clientTimeTruncated := t.Truncate(time.Second)

			// If the cached time is not after the client's time, it's not modified
			// Use !After for correct comparison including equality
			if !modTimeTruncated.After(clientTimeTruncated) {
				w.WriteHeader(http.StatusNotModified)
				return true
			}
		} else {
			logging.Warn("Could not parse If-Modified-Since header '%s': %v", ifims, err)
		}
	}

	// ETag handling could be added here if needed, checking If-None-Match

	return false
}

// isClientDisconnectedError checks if an error indicates the client disconnected.
func isClientDisconnectedError(err error) bool {
	if err == nil {
		return false
	}
	// Check for common client disconnection errors across different OS
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	// Check for context cancellation (might happen if client closes connection)
	if errors.Is(err, context.Canceled) {
		return true
	}
	// Check for specific net/http error strings (less reliable but common)
	errStr := err.Error()
	if strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "client disconnected") ||
		// Go 1.17+ specific error on tls connection close:
		strings.Contains(errStr, "tls: user canceled") {
		return true
	}
	return false
}
