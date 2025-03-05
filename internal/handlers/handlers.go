package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/utils"
)

// ServerConfig definition has been moved to server_config.go

// requestLock provides a mechanism to prevent concurrent requests for the same resource.
// This now also handles the "currentlyCaching" state.
var requestLock = struct {
	sync.RWMutex
	inProgress map[string]*cacheRequest
}{inProgress: make(map[string]*cacheRequest)}

// cacheRequest holds the state for a single in-progress cache request.
type cacheRequest struct {
	done chan struct{} // Channel to signal completion.
}

// List of allowed response headers
var allowedResponseHeaders = map[string]bool{
	"Content-Type":   true,
	"Date":           true,
	"Etag":           true,
	"Last-Modified":  true,
	"Content-Length": true,
}

// clientCache stores HTTP clients to avoid creating new ones for each request
var clientCache = struct {
	sync.RWMutex
	clients map[int]*http.Client
}{clients: make(map[int]*http.Client)}

// filterAndSetHeaders sets only allowed headers from the source headers to response writer
func filterAndSetHeaders(w http.ResponseWriter, headers http.Header) {
	for header, values := range headers {
		if allowedResponseHeaders[http.CanonicalHeaderKey(header)] {
			for _, value := range values {
				w.Header().Add(header, value)
			}
		}
	}
}

// acquireLock tries to acquire a lock for a resource path.
// It returns:
// - true if the lock was acquired (no other request is in progress).
// - false if the lock is already held (another request is in progress).
// - a channel that will be closed when the lock is released.
// This function now atomically handles both locking and the "currently caching" state.
func acquireLock(path string) (bool, chan struct{}) {
	requestLock.Lock()
	defer requestLock.Unlock()

	if req, exists := requestLock.inProgress[path]; exists {
		// Another request is already in progress; return false and the existing channel.
		return false, req.done
	}

	// No other request is in progress; create a new cacheRequest and acquire the lock.
	req := &cacheRequest{done: make(chan struct{})}
	requestLock.inProgress[path] = req
	return true, req.done
}

// releaseLock releases the lock for a resource path and notifies waiters.
func releaseLock(path string) {
	requestLock.Lock()
	defer requestLock.Unlock()

	if req, exists := requestLock.inProgress[path]; exists {
		close(req.done) // Notify all waiters that the request is complete.
		delete(requestLock.inProgress, path)
	}
}

// Common HTTP request handling functions to avoid duplication

// validateRequest checks if the request method and query parameters are valid
func validateRequest(w http.ResponseWriter, r *http.Request) bool {
	// Only handle GET and HEAD requests
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return false
	}

	// Check for query parameters (not allowed)
	if r.URL.RawQuery != "" {
		http.Error(w, "Query parameters are not allowed", http.StatusForbidden)
		return false
	}

	return true
}

// getClient returns the HTTP client to use for requests
func getClient(config ServerConfig) *http.Client {
	if config.Client != nil {
		return config.Client
	}

	// Use a longer timeout for GET requests to handle large files
	timeout := 120 // Increased timeout to 120 seconds

	// Check if we already have a client with this timeout
	clientCache.RLock()
	client, exists := clientCache.clients[timeout]
	clientCache.RUnlock()

	if exists {
		return client
	}

	// Create a new client and cache it
	client = utils.CreateHTTPClient(timeout)

	clientCache.Lock()
	clientCache.clients[timeout] = client
	clientCache.Unlock()

	return client
}

// getRemotePath converts local path to remote path
func getRemotePath(config ServerConfig, localPath string) string {
	// Remove local path prefix to get the remote path
	remotePath := strings.TrimPrefix(localPath, config.LocalPath)

	// Ensure path doesn't start with a slash to avoid double slashes when concatenating with upstream URL
	remotePath = strings.TrimPrefix(remotePath, "/")

	return remotePath
}

// fetchFromUpstream fetches a resource from the upstream server
func fetchFromUpstream(config ServerConfig, r *http.Request, upstreamURL string) (*http.Response, error) {
	// Create request to upstream server
	req, err := http.NewRequest(r.Method, upstreamURL, nil)
	if err != nil {
		return nil, err
	}

	// For rarely changing files, we can minimize headers to reduce overhead
	fileType := utils.GetFilePatternType(r.URL.Path)
	if fileType == utils.TypeRarelyChanging {
		// For rarely changing files, we only need essential headers
		if r.Header.Get("Range") != "" {
			req.Header.Set("Range", r.Header.Get("Range"))
		}
		if r.Header.Get("If-Modified-Since") != "" {
			req.Header.Set("If-Modified-Since", r.Header.Get("If-Modified-Since"))
		}
		if r.Header.Get("If-None-Match") != "" {
			req.Header.Set("If-None-Match", r.Header.Get("If-None-Match"))
		}
	} else {
		// For frequently changing files, copy all relevant headers
		copyRelevantHeaders(req, r)
	}

	// Add User-Agent header to identify our application
	req.Header.Set("User-Agent", "Debian APT-HTTP/1.3 (2.2.4)")

	// Set accept-encoding to support compression if client supports it
	if r.Header.Get("Accept-Encoding") != "" {
		req.Header.Set("Accept-Encoding", r.Header.Get("Accept-Encoding"))
	} else {
		// By default accept gzip and deflate
		req.Header.Set("Accept-Encoding", "gzip, deflate")
	}

	// Get client with optimized settings
	client := getClient(config)

	// Set timeout in context - use a shorter timeout for HEAD requests
	var timeout time.Duration
	if r.Method == http.MethodHead {
		timeout = 10 * time.Second
	} else {
		timeout = 60 * time.Second // Longer timeout for GET requests to allow for large files
	}

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	req = req.WithContext(ctx)

	// Stream response from upstream
	return client.Do(req)
}

// copyRelevantHeaders copies relevant headers from the original request to the upstream request
func copyRelevantHeaders(upstreamReq *http.Request, originalReq *http.Request) {
	// Headers to copy
	relevantHeaders := []string{
		"If-Modified-Since",
		"If-None-Match",
		"Range",
		"Cache-Control",
	}

	for _, header := range relevantHeaders {
		if value := originalReq.Header.Get(header); value != "" {
			upstreamReq.Header.Set(header, value)
		}
	}
}

// updateCache updates both the content cache and the header cache
func updateCache(config ServerConfig, path string, body []byte, lastModified time.Time, headers http.Header) {
	// Create a separate context for background caching operations with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // Increased timeout
	defer cancel()

	// No need for currentlyCaching map anymore, as acquireLock handles this.

	// Use a WaitGroup to coordinate header and content caching
	var wg sync.WaitGroup
	wg.Add(2)

	// Track errors
	var headerErr, contentErr error

	// Update header cache in a goroutine
	go func() {
		defer wg.Done()

		// Store original headers from upstream server in header cache
		headerErr = config.HeaderCache.PutHeaders(path, headers)
	}()

	// Update content cache in a goroutine
	go func() {
		defer wg.Done()

		// Update content cache
		contentErr = config.Cache.Put(path, bytes.NewReader(body), int64(len(body)), lastModified)
	}()

	// Use a channel to handle timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		// Both operations completed
		if headerErr != nil {
			logging.Error("Error storing headers in cache: %v", headerErr)
		} else if config.LogRequests {
			logging.Info("Stored headers in cache: %s", path)
		}

		if contentErr != nil {
			logging.Error("Error storing in cache: %v", contentErr)
		} else if config.LogRequests {
			logging.Info("Stored in cache: %s (%d bytes)", path, len(body))
		}

		// Explicitly clear the body to help garbage collection
		body = nil
	case <-ctx.Done():
		logging.Error("Cache update timed out for: %s", path)
		// Explicitly clear the body to help garbage collection
		body = nil
	}
}

// sendNotModified sends a 304 Not Modified response.
func sendNotModified(w http.ResponseWriter, config ServerConfig, r *http.Request) {
	if config.LogRequests {
		logging.Info("Resource not modified: %s", r.URL.Path)
	}
	w.WriteHeader(http.StatusNotModified)
}

// checkAndHandleIfModifiedSince checks the If-Modified-Since header and handles it.
// Returns true if a response was sent (either 304 or because of an error), false otherwise.
func checkAndHandleIfModifiedSince(w http.ResponseWriter, r *http.Request, lastModifiedStr string, lastModifiedTime time.Time, config ServerConfig) bool {
	ifModifiedSince := r.Header.Get("If-Modified-Since")
	if ifModifiedSince == "" {
		return false // No If-Modified-Since header, nothing to do
	}

	ifModifiedSinceTime, err := time.Parse(http.TimeFormat, ifModifiedSince)
	if err != nil {
		if config.LogRequests {
			logging.Error("Failed to parse If-Modified-Since header: %s, error: %v", ifModifiedSince, err)
		}
		return false // Treat as if the header wasn't sent
	}

	var lastModifiedTimeToCheck time.Time
	if lastModifiedStr != "" {
		lastModifiedTimeToCheck, err = time.Parse(http.TimeFormat, lastModifiedStr)
		if err != nil {
			// If we can't parse the header, use the file's lastModified time
			lastModifiedTimeToCheck = lastModifiedTime
		}
	} else {
		lastModifiedTimeToCheck = lastModifiedTime
	}

	if !lastModifiedTimeToCheck.After(ifModifiedSinceTime) {
		sendNotModified(w, config, r)
		return true // We sent a 304 response
	}

	return false
}

// validateWithUpstream checks with the upstream server if the cached copy is still valid
func validateWithUpstream(config ServerConfig, r *http.Request, cachedHeaders http.Header, lastModified time.Time) (bool, error) {
	// Check file type to determine if we need to validate with upstream
	fileType := utils.GetFilePatternType(r.URL.Path)

	// For rarely changing files (like .deb packages), we can skip validation
	// They almost never change once published
	if fileType == utils.TypeRarelyChanging {
		if config.LogRequests {
			logging.Info("Skipping upstream validation for rarely changing file: %s", r.URL.Path)
		}
		return true, nil
	}

	remotePath := getRemotePath(config, r.URL.Path)
	// Ensure we don't have double slashes in the URL
	upstreamURL := fmt.Sprintf("%s%s", config.UpstreamURL, remotePath)
	req, err := http.NewRequest(http.MethodHead, upstreamURL, nil)
	if err != nil {
		return false, fmt.Errorf("error creating HEAD request for validation: %w", err)
	}

	// Always send If-Modified-Since when validating with upstream
	lastModifiedStr := cachedHeaders.Get("Last-Modified")
	if lastModifiedStr == "" {
		lastModifiedStr = lastModified.Format(http.TimeFormat)
	}
	req.Header.Set("If-Modified-Since", lastModifiedStr)
	req.Header.Set("User-Agent", "Debian APT-HTTP/1.3 (2.2.4)")

	if config.LogRequests {
		logging.Info("Validating cached file with upstream: %s", r.URL.Path)
	}

	client := getClient(config)
	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("error checking with upstream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		if config.LogRequests {
			logging.Info("Upstream confirms cache is still valid: %s", r.URL.Path)
		}
		return true, nil
	}

	// If status is not 304, it means the file has been modified
	// We'll treat this as cache invalidation
	return false, nil
}

// handleCacheHit handles a cache hit, returning true if the response was handled
func handleCacheHit(w http.ResponseWriter, r *http.Request, config ServerConfig, content io.ReadCloser, contentLength int64, lastModified time.Time, useIfModifiedSince bool) bool {
	defer content.Close()

	cachedHeaders, headerErr := config.HeaderCache.GetHeaders(r.URL.Path)
	if headerErr != nil {
		// Fallback to basic headers if no cached headers
		logging.Error("No cached headers found for %s: %v", r.URL.Path, headerErr)
		setBasicHeaders(w, r, nil, lastModified, useIfModifiedSince, config)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", contentLength))

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return true
		}
		_, err := io.Copy(w, content)
		if err != nil {
			logging.Error("Error writing response: %v", err)
		}
		return true
	}

	// Check If-Modified-Since
	lastModifiedStr := cachedHeaders.Get("Last-Modified")
	if useIfModifiedSince && checkAndHandleIfModifiedSince(w, r, lastModifiedStr, lastModified, config) {
		return true
	}

	// Validate with upstream if needed
	fileType := utils.GetFilePatternType(r.URL.Path)

	// Only validate frequently changing files with upstream
	if useIfModifiedSince && fileType == utils.TypeFrequentlyChanging {
		validationKey := fmt.Sprintf("validation:%s", r.URL.Path)
		isValid, _ := config.ValidationCache.Get(validationKey)

		// Check with upstream if validation cache is invalid or expired
		if !isValid {
			cacheIsValid, err := validateWithUpstream(config, r, cachedHeaders, lastModified)
			if err != nil {
				logging.Error("Error validating with upstream: %v", err)
				// If validation fails, we still serve the cached content, but log the error
			} else {
				if cacheIsValid {
					// Cache is still valid, update validation cache
					config.ValidationCache.Put(validationKey, time.Now())

					// If client sent If-Modified-Since, check if we need to send 304
					if r.Header.Get("If-Modified-Since") != "" {
						if checkAndHandleIfModifiedSince(w, r, lastModifiedStr, lastModified, config) {
							return true
						}
					}
				} else {
					// Content has been modified on upstream, treat as cache miss
					if config.LogRequests {
						logging.Info("Content modified on upstream, invalidating cache: %s", r.URL.Path)
					}
					return false
				}
			}
		} else {
			// We have a valid cache entry, check if client needs update
			if r.Header.Get("If-Modified-Since") != "" {
				if checkAndHandleIfModifiedSince(w, r, lastModifiedStr, lastModified, config) {
					return true
				}
			}
		}
	} else if fileType == utils.TypeRarelyChanging {
		// For rarely changing files, we can assume they're valid for much longer
		// Just check if client needs a 304 Not Modified response
		if r.Header.Get("If-Modified-Since") != "" {
			if checkAndHandleIfModifiedSince(w, r, lastModifiedStr, lastModified, config) {
				return true
			}
		}
	}

	// If we reach here, serve the cached content

	// Use cached headers exactly as they were stored, but only allowed ones
	filterAndSetHeaders(w, cachedHeaders)

	// Write status code and body (if not a HEAD request)
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		// Stream the file directly to the client instead of loading it into memory
		_, err := io.Copy(w, content)
		if err != nil {
			// Check if the error is due to client disconnection
			if strings.Contains(err.Error(), "context canceled") ||
				strings.Contains(err.Error(), "connection reset by peer") ||
				strings.Contains(err.Error(), "broken pipe") {
				// This is an expected error when client disconnects
				if config.LogRequests {
					logging.Info("Client disconnected during download: %s", r.URL.Path)
				}
				return true
			}
			logging.Error("Error streaming response: %v", err)
		}
	}
	return true
}

// handleCacheMiss handles the case when the requested resource is not in cache
func handleCacheMiss(w http.ResponseWriter, r *http.Request, config ServerConfig, useIfModifiedSince bool) {
	path := r.URL.Path

	// Acquire lock for this resource.  This also handles the "currently caching" state.
	acquired, done := acquireLock(path)
	if !acquired {
		// Another request is already fetching this resource; wait for it to complete.
		if config.LogRequests {
			logging.Info("Waiting for another request to fetch: %s", path)
		}
		<-done

		// After waiting, try to get the resource from the cache again.
		content, contentLength, lastModified, err := config.Cache.Get(path)
		if err == nil {
			// Another request has fetched this resource and put it in the cache.
			if config.LogRequests {
				logging.Info("Resource was fetched by another request: %s", path)
			}
			handleCacheHit(w, r, config, content, contentLength, lastModified, useIfModifiedSince)
			return
		}

		// If it's *still* not in the cache, something went wrong with the other request.
		// We *could* try to acquire the lock again and fetch, but to avoid potential
		// infinite loops, it's better to just return an error.
		logging.Error("Resource not found in cache after waiting for another request: %s", path)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// We've acquired the lock; make sure to release it when done.
	defer releaseLock(path)

	remotePath := getRemotePath(config, path)
	// Ensure we don't have double slashes in the URL
	upstreamURL := fmt.Sprintf("%s%s", config.UpstreamURL, remotePath)

	// For HEAD requests, we handle things differently
	if r.Method == http.MethodHead {
		handleHeadRequest(w, r, config, upstreamURL, path)
		return
	}

	// For GET requests, stream content directly
	handleGetRequest(w, r, config, upstreamURL, path)
}

// handleHeadRequest handles HEAD requests from clients
func handleHeadRequest(w http.ResponseWriter, r *http.Request, config ServerConfig, upstreamURL string, path string) {
	// Fetch from upstream
	resp, err := fetchFromUpstream(config, r, upstreamURL)
	if err != nil {
		// If HEAD request fails, fall back to GET request
		logging.Error("HEAD request failed, falling back to GET: %v", err)
		getFullContent(w, r, config, upstreamURL, path)
		return
	}
	defer resp.Body.Close()

	// Get last modified time
	lastModifiedTime := time.Now()
	if lastModifiedHeader := resp.Header.Get("Last-Modified"); lastModifiedHeader != "" {
		if parsedTime, err := time.Parse(http.TimeFormat, lastModifiedHeader); err == nil {
			lastModifiedTime = parsedTime
		}
	}

	// Set headers and status code
	filterAndSetHeaders(w, resp.Header)
	w.WriteHeader(resp.StatusCode)

	// Read response body to cache it, but don't send to client (it's a HEAD request)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logging.Error("Error reading response from upstream for HEAD request: %v", err)
		return
	}

	// Update cache in background
	go updateCache(config, path, body, lastModifiedTime, resp.Header)

	// Clear body to help garbage collection
	body = nil
}

// handleGetRequest handles GET requests, optimized for APT clients
func handleGetRequest(w http.ResponseWriter, r *http.Request, config ServerConfig, upstreamURL string, path string) {
	// Get client with optimized settings
	client := getClient(config)

	// Make a direct GET request to upstream
	getReq, _ := http.NewRequest(http.MethodGet, upstreamURL, nil)
	copyRelevantHeaders(getReq, r)
	getReq.Header.Set("User-Agent", "Debian APT-HTTP/1.3 (2.2.4)")

	getResp, err := client.Do(getReq)
	if err != nil {
		http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
		logging.Error("Error fetching content from upstream: %v", err)
		return
	}
	defer getResp.Body.Close()

	if getResp.StatusCode == http.StatusNotModified {
		// Resource not modified (This should only happen if If-Modified-Since was sent)
		if config.LogRequests {
			logging.Info("Upstream reports resource not modified: %s", path)
		}

		// Store validation result in validation cache
		validationKey := fmt.Sprintf("validation:%s", path)
		config.ValidationCache.Put(validationKey, time.Now())
		if config.LogRequests {
			logging.Info("Stored validation result in cache for: %s", path)
		}

		sendNotModified(w, config, r)
		return
	}

	if getResp.StatusCode != http.StatusOK {
		// Forward error status from upstream
		logging.Error("Unexpected status code from upstream GET: %d, URL: %s", getResp.StatusCode, upstreamURL)

		// Set response headers from upstream
		filterAndSetHeaders(w, getResp.Header)

		w.WriteHeader(getResp.StatusCode)
		io.Copy(w, getResp.Body)
		return
	}

	// Get last modified time
	lastModifiedTime := time.Now()
	if lastModifiedHeader := getResp.Header.Get("Last-Modified"); lastModifiedHeader != "" {
		if parsedTime, err := time.Parse(http.TimeFormat, lastModifiedHeader); err == nil {
			lastModifiedTime = parsedTime
		}
	}

	// Set allowed response headers
	filterAndSetHeaders(w, getResp.Header)

	// Ensure Content-Length is set if available
	if getResp.ContentLength > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", getResp.ContentLength))
	}

	// Write status code
	w.WriteHeader(http.StatusOK)

	// Create a buffer to store the content for caching
	var buf bytes.Buffer
	defer buf.Reset() // Ensure buffer is reset even on early returns

	// Use a MultiWriter to write to both the client and our buffer
	mw := io.MultiWriter(w, &buf)

	// Stream the content directly to the client while also capturing it for caching
	_, err = io.Copy(mw, getResp.Body)
	if err != nil {
		// Check if the error is due to client disconnection/context cancellation
		if strings.Contains(err.Error(), "context canceled") ||
			strings.Contains(err.Error(), "connection reset by peer") ||
			strings.Contains(err.Error(), "broken pipe") {
			// This is an expected error when client disconnects
			if config.LogRequests {
				logging.Info("Client disconnected during download: %s", path)
			}
			// Don't update cache if client disconnected to avoid corrupted cache
			return
		} else {
			logging.Error("Error streaming response to client: %v", err)
		}
	}

	// Get buffer bytes for caching
	bufBytes := buf.Bytes()

	// Update cache in background with the captured content
	go updateCache(config, path, bufBytes, lastModifiedTime, getResp.Header)
}

// getFullContent fetches the full content directly when HEAD request fails
func getFullContent(w http.ResponseWriter, r *http.Request, config ServerConfig, upstreamURL string, path string) {
	// Fetch from upstream
	resp, err := fetchFromUpstream(config, r, upstreamURL)
	if err != nil {
		http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
		logging.Error("Error fetching from upstream: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		// Resource not modified (This should only happen if If-Modified-Since was sent)
		if config.LogRequests {
			logging.Info("Upstream reports resource not modified: %s", path)
		}

		// Store validation result in validation cache
		validationKey := fmt.Sprintf("validation:%s", path)
		config.ValidationCache.Put(validationKey, time.Now())
		if config.LogRequests {
			logging.Info("Stored validation result in cache for: %s", path)
		}

		sendNotModified(w, config, r)
		return
	}

	if resp.StatusCode != http.StatusOK {
		// Forward error status from upstream
		logging.Error("Unexpected status code from upstream: %d, URL: %s", resp.StatusCode, upstreamURL)

		// Set response headers from upstream
		filterAndSetHeaders(w, resp.Header)

		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Get last modified time
	lastModifiedTime := time.Now()
	if lastModifiedHeader := resp.Header.Get("Last-Modified"); lastModifiedHeader != "" {
		if parsedTime, err := time.Parse(http.TimeFormat, lastModifiedHeader); err == nil {
			lastModifiedTime = parsedTime
		}
	}

	// Set allowed response headers
	filterAndSetHeaders(w, resp.Header)

	// Set Content-Length if available
	if resp.ContentLength > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", resp.ContentLength))
	}

	// Write status code
	w.WriteHeader(http.StatusOK)

	// Create a buffer to store the content for caching
	var buf bytes.Buffer
	defer buf.Reset() // Ensure buffer is reset even on early returns.

	// Use a MultiWriter to write to both the client and our buffer
	mw := io.MultiWriter(w, &buf)

	// Stream the content directly to the client while also capturing it for caching
	_, err = io.Copy(mw, resp.Body)
	if err != nil {
		if strings.Contains(err.Error(), "context canceled") ||
			strings.Contains(err.Error(), "connection reset by peer") ||
			strings.Contains(err.Error(), "broken pipe") {
			// This is an expected error when client disconnects
			if config.LogRequests {
				logging.Info("Client disconnected during download: %s", path)
			}
			// Don't update cache if client disconnected to avoid corrupted cache
			return
		} else {
			logging.Error("Error streaming response to client: %v", err)
		}
	}

	// Get buffer bytes for caching
	bufBytes := buf.Bytes()

	// Update cache in background with the captured content
	go updateCache(config, path, bufBytes, lastModifiedTime, resp.Header)
}

// setBasicHeaders sets basic headers when cached headers are not available
func setBasicHeaders(w http.ResponseWriter, r *http.Request, _ http.Header, lastModified time.Time, useIfModifiedSince bool, config ServerConfig) {
	// For directory URLs, always use text/html
	if strings.HasSuffix(r.URL.Path, "/") {
		w.Header().Set("Content-Type", "text/html")
	} else {
		// Set Content-Type, only if not already set
		if w.Header().Get("Content-Type") == "" {
			contentType := utils.GetContentType(r.URL.Path)
			if contentType != "" {
				w.Header().Set("Content-Type", contentType)
			}
		}
	}
	w.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))

	// Check If-Modified-Since header only if we should use it for this file type
	if useIfModifiedSince && checkAndHandleIfModifiedSince(w, r, "", lastModified, config) {
		return
	}
}

// HandleRequest is a common handler for all types of requests
func HandleRequest(config ServerConfig, useIfModifiedSince bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if config.LogRequests {
			logging.Info("Request: %s", r.URL.Path)
		}

		if !validateRequest(w, r) {
			return
		}

		// Check if we have a recent validation result in the validation cache first
		if useIfModifiedSince && r.Header.Get("If-Modified-Since") != "" {
			validationKey := fmt.Sprintf("validation:%s", r.URL.Path)
			isValid, _ := config.ValidationCache.Get(validationKey)
			if isValid {
				if config.LogRequests {
					logging.Info("Using cached validation result for: %s", r.URL.Path)
				}
				sendNotModified(w, config, r)
				return
			}
		}

		// Try to get from cache
		content, contentLength, lastModified, err := config.Cache.Get(r.URL.Path)
		if err == nil {
			// Cache hit
			if config.LogRequests {
				logging.Info("Cache hit for: %s", r.URL.Path)
			}
			if handleCacheHit(w, r, config, content, contentLength, lastModified, useIfModifiedSince) {
				return
			}
		}

		// Cache miss or validation needed
		handleCacheMiss(w, r, config, useIfModifiedSince)
	}
}

// HandleRelease handles requests for release files
// These are cached in storage and use If-Modified-Since when checking with upstream
func HandleRelease(config ServerConfig) http.HandlerFunc {
	return HandleRequest(config, true)
}

// HandleCacheableRequest handles requests for cacheable files
// These are cached in storage but use If-Modified-Since based on file type
func HandleCacheableRequest(config ServerConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Determine if this file should use If-Modified-Since based on its type
		fileType := utils.GetFilePatternType(r.URL.Path)
		HandleRequest(config, fileType == utils.TypeFrequentlyChanging)(w, r)
	}
}
