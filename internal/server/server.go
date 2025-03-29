// internal/server/server.go
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/fetch"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

// Server wraps the http.Server and manages routing.
type Server struct {
	*http.Server
	cfg          *config.Config
	cacheManager cache.CacheManager
	fetcher      *fetch.Coordinator
}

// New creates a new Server instance.
func New(
	cfg *config.Config,
	cacheManager cache.CacheManager,
	fetcher *fetch.Coordinator,
) (*Server, error) {
	mux := http.NewServeMux()

	// --- Status Endpoint ---
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		// More detailed status could be added later (e.g., cache stats)
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, "OK")
		cacheStats := cacheManager.Stats()
		fmt.Fprintf(w, "Cache Items: %d\n", cacheStats.ItemCount)
		fmt.Fprintf(w, "Cache Size: %s / %s\n", util.FormatSize(cacheStats.CurrentSize), util.FormatSize(cacheStats.MaxSize))

	})

	// --- Repository Handlers ---
	for _, repo := range cfg.Repositories {
		if !repo.Enabled {
			logging.Info("Skipping disabled repository: %s", repo.Name)
			continue
		}

		// Ensure path starts and ends with a slash for StripPrefix compatibility
		// The repo name itself is the primary path component.
		pathPrefix := "/" + repo.Name + "/"

		repoHandler := NewRepositoryHandler(repo, cfg.Server, cacheManager, fetcher)

		// StripPrefix removes /<repoName>/ before passing to the handler
		mux.Handle(pathPrefix, http.StripPrefix("/"+repo.Name, repoHandler))
		logging.Info("Registered handler for repository %q at path %s (Upstream: %s)", repo.Name, pathPrefix, repo.URL)
	}

	// --- Middleware ---
	var handler http.Handler = mux
	handler = LoggingMiddleware(handler)  // Apply logging first
	handler = RecoveryMiddleware(handler) // Apply recovery

	httpServer := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.Duration(),
		IdleTimeout:       cfg.Server.IdleTimeout.Duration(),
		// Set other timeouts like WriteTimeout if needed, but streaming makes it complex.
		// Rely on IdleTimeout and ReadHeaderTimeout primarily.
	}

	return &Server{
		Server:       httpServer, // Embed http.Server
		cfg:          cfg,
		cacheManager: cacheManager,
		fetcher:      fetcher,
	}, nil
}

// RepositoryHandler handles requests for a specific repository.
type RepositoryHandler struct {
	repoConfig   config.Repository
	serverConfig config.ServerConfig
	cacheManager cache.CacheManager
	fetcher      *fetch.Coordinator
}

// NewRepositoryHandler creates a handler for a single repository.
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

// ServeHTTP handles incoming requests for the repository.
func (h *RepositoryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	// The path received here has the /<repoName> prefix already stripped.
	// We need to reconstruct the cache key and upstream path.
	// Ensure leading slash is present for consistency.
	filePath := strings.TrimPrefix(r.URL.Path, "/")
	if filePath == "" && strings.HasSuffix(r.URL.Path, "/") {
		// Handle root request within the repository (e.g., /ubuntu/) -> /
		// This might need special handling if directories need listing (not implemented)
		// For now, treat it like any other file path, resulting in key "ubuntu/"
		// Or, perhaps reject directory listings?
		http.Error(w, "Directory listing not supported", http.StatusForbidden)
		return
	}

	// Construct the unique cache key: "repoName/path/to/file"
	cacheKey := h.repoConfig.Name + "/" + filePath
	upstreamURL := h.repoConfig.URL + "/" + filePath // Assuming upstream expects path appended directly

	// 1. Check Cache Validation (for frequently changing files)
	//    Simple approach: Only validate HEAD if If-Modified-Since/If-None-Match is present.
	//    Advanced: Use Validation Cache based on file patterns.
	useValidationCache := true // Check file patterns later if needed
	if useValidationCache {
		validationTime, ok := h.cacheManager.GetValidation(cacheKey)
		if ok {
			// Check client headers against cached validation time
			if h.checkClientCacheHeaders(w, r, validationTime) {
				// Client cache is valid based on our cached validation time
				return // 304 sent by checkClientCacheHeaders
			}
			// Client cache is stale, or no headers provided, proceed to check our cache
		}
	}

	// 2. Check Local Cache
	cacheReader, cacheSize, cacheModTime, err := h.cacheManager.Get(r.Context(), cacheKey)
	if err == nil {
		defer cacheReader.Close()
		logging.Debug("Cache hit for key: %s", cacheKey)

		// Check If-Modified-Since / If-None-Match from client request
		if h.checkClientCacheHeaders(w, r, cacheModTime) {
			// Client cache is valid based on file mod time
			return // 304 sent by checkClientCacheHeaders
		}

		// Serve from cache
		w.Header().Set("Last-Modified", cacheModTime.UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", cacheSize))
		// Add ETag if available/generated
		// Add Content-Type based on file extension
		w.Header().Set("Content-Type", util.GetContentType(filePath))

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}

		http.ServeContent(w, r, filePath, cacheModTime, cacheReader.(io.ReadSeeker)) // ServeContent handles Range requests
		return
	}

	if !errors.Is(err, os.ErrNotExist) {
		// Unexpected cache error
		logging.Error("Error reading from cache for key %s: %v", cacheKey, err)
		http.Error(w, "Internal Cache Error", http.StatusInternalServerError)
		return
	}

	// 3. Cache Miss - Fetch from Upstream
	logging.Debug("Cache miss for key: %s, fetching from upstream: %s", cacheKey, upstreamURL)

	fetchResult, err := h.fetcher.Fetch(r.Context(), cacheKey, upstreamURL, r.Header)
	if err != nil {
		logging.Error("Failed to fetch %s (key %s) from upstream: %v", upstreamURL, cacheKey, err)
		// Distinguish between upstream errors (e.g., 404) and network errors
		if errors.Is(err, fetch.ErrNotFound) {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		} else if errors.Is(err, fetch.ErrUpstreamNotModified) {
			// This case should ideally be handled *before* the fetch call
			// by sending a conditional request. If we get here, it's likely
			// due to a race or mismatch. Sending 304 is plausible.
			w.WriteHeader(http.StatusNotModified)
		} else {
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		}
		return
	}
	defer fetchResult.Body.Close() // Ensure response body is always closed

	// Update validation cache if successful fetch
	h.cacheManager.PutValidation(cacheKey, time.Now())

	// Check client cache headers *again* against the *upstream* response headers
	// This handles the case where the client had a stale version, but the upstream
	// indicates the client's version is *now* valid (unlikely but possible).
	// Simpler: Assume fetch means client needs the new content.

	// Set headers from upstream response
	util.CopyRelevantHeaders(w.Header(), fetchResult.Header)
	w.Header().Set("Content-Type", util.GetContentType(filePath)) // Override if necessary
	// Ensure Content-Length is set if upstream provided it
	if cl := fetchResult.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}

	// Create reader for caching
	// Use TeeReader to write to cache *while* sending to client
	pr, pw := io.Pipe()
	cacheErrChan := make(chan error, 1)

	go func() {
		defer close(cacheErrChan)
		// Note: fetchResult.ModTime might be zero if header missing/invalid
		modTimeToUse := fetchResult.ModTime
		if modTimeToUse.IsZero() {
			modTimeToUse = time.Now() // Fallback if upstream doesn't provide Last-Modified
		}
		err := h.cacheManager.Put(context.Background(), cacheKey, pr, fetchResult.Size, modTimeToUse) // Use background context for cache write
		if err != nil {
			logging.Error("Failed to write key %s to cache: %v", cacheKey, err)
			cacheErrChan <- err // Signal error
		}
	}()

	// TeeReader reads from fetchResult.Body, writes reads to pw (for cache),
	// and returns a reader for the client.
	teeReader := io.TeeReader(fetchResult.Body, pw)

	// Write header *before* starting to stream body
	w.WriteHeader(fetchResult.StatusCode)

	if r.Method == http.MethodHead {
		// Already wrote headers, nothing more to do for HEAD
		pw.Close() // Close pipe writer immediately if HEAD
	} else {
		// Stream response to client
		_, copyErr := io.Copy(w, teeReader)

		// Close the pipe writer *after* copying to client is done (or failed)
		// This signals the cache Put operation that all data has been written.
		pipeCloseErr := pw.Close() // Close can return error if reader already closed

		// Check for errors
		if copyErr != nil {
			// Error copying to client (e.g., client disconnected)
			// Cache write might still be happening or partially complete.
			logging.Warn("Error streaming response to client for %s: %v", cacheKey, copyErr)
			// Allow cache write goroutine to finish, but log the client error.
		}
		if pipeCloseErr != nil {
			logging.Warn("Error closing pipe writer for %s: %v", cacheKey, pipeCloseErr)
		}

	}

	// Wait for the cache write goroutine to finish and check its error status
	cacheErr := <-cacheErrChan
	if cacheErr != nil {
		// Cache write failed *after* potentially sending data to client.
		// Difficult to recover cleanly. Log it. The cache entry might be corrupt.
		// Consider deleting the possibly corrupt cache entry.
		logging.Error("Asynchronous cache write failed for %s: %v. Potential cache inconsistency.", cacheKey, cacheErr)
		_ = h.cacheManager.Delete(context.Background(), cacheKey)
	} else {
		logging.Debug("Successfully fetched and streamed %s", cacheKey)
	}
}

// checkClientCacheHeaders checks If-Modified-Since/If-None-Match request headers
// against the provided modTime. If the client cache is valid, it writes a 304
// Not Modified response and returns true. Otherwise, returns false.
func (h *RepositoryHandler) checkClientCacheHeaders(w http.ResponseWriter, r *http.Request, modTime time.Time) bool {
	if modTime.IsZero() {
		return false // Cannot validate without a modification time
	}

	// ETag check first if available (more robust) - TBD if we generate/store ETags

	// If-Modified-Since check
	ifims := r.Header.Get("If-Modified-Since")
	if ifims != "" {
		if t, err := http.ParseTime(ifims); err == nil {
			// According to RFC 7232 section 3.3, If-Modified-Since comparison
			// should ignore sub-second precision.
			modTimeTruncated := modTime.Truncate(time.Second)
			if !modTimeTruncated.After(t.Truncate(time.Second)) {
				// Not modified
				w.WriteHeader(http.StatusNotModified)
				return true
			}
		}
	}

	return false
}
