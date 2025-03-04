package handlers

import (
	"bytes"
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

// requestLock provides a mechanism to prevent concurrent requests for the same resource
// This helps prevent the "thundering herd" problem where multiple clients request the same
// uncached resource simultaneously
var requestLock = struct {
	sync.RWMutex
	inProgress map[string]chan struct{}
}{inProgress: make(map[string]chan struct{})}

// validationCacheKeyFormat is the format string for validation cache keys.
const validationCacheKeyFormat = "validation:%s"

// List of allowed response headers
var allowedResponseHeaders = map[string]bool{
	"Content-Type":   true,
	"Date":           true,
	"Etag":           true,
	"Last-Modified":  true,
	"Content-Length": true,
}

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

// acquireLock tries to acquire a lock for a resource path
// Returns true if the lock was acquired, false if it's already locked
// If it's already locked, the caller should wait on the returned channel
func acquireLock(path string) (bool, chan struct{}) {
	requestLock.RLock()
	ch, exists := requestLock.inProgress[path]
	requestLock.RUnlock()

	if exists {
		return false, ch
	}

	requestLock.Lock()
	defer requestLock.Unlock()

	// Check again in case another goroutine acquired the lock
	// between our RUnlock and Lock
	ch, exists = requestLock.inProgress[path]
	if exists {
		return false, ch
	}

	// Create a new channel and acquire the lock
	ch = make(chan struct{})
	requestLock.inProgress[path] = ch
	return true, ch
}

// releaseLock releases the lock for a resource path and notifies waiters
func releaseLock(path string) {
	requestLock.Lock()
	defer requestLock.Unlock()

	if ch, exists := requestLock.inProgress[path]; exists {
		close(ch) // Notify all waiters
		delete(requestLock.inProgress, path)
	}
}

// reacquireLock attempts to re-acquire the lock after waiting on the channel.
func reacquireLock(path string) bool {
	acquired, _ := acquireLock(path)
	return acquired
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
	return utils.CreateHTTPClient(60) // Default 60 second timeout
}

// getRemotePath converts local path to remote path
func getRemotePath(config ServerConfig, localPath string) string {
	// Remove local path prefix to get the remote path
	remotePath := strings.TrimPrefix(localPath, config.LocalPath)

	// Ensure path doesn't start with a slash to avoid double slashes when concatenating with upstream URL
	remotePath = strings.TrimPrefix(remotePath, "/")

	return remotePath
}

// fetchFromUpstream fetches content from the upstream server
func fetchFromUpstream(config ServerConfig, r *http.Request, upstreamURL string) (*http.Response, error) {
	// Log the request
	logging.Info("Fetching from upstream: %s", upstreamURL)

	// Create a new request
	req, err := http.NewRequest(r.Method, upstreamURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request to upstream: %w", err)
	}

	// Copy relevant headers from the client request to the upstream request
	req.Header.Set("User-Agent", "Go-APT-Cache/1.0")
	if ifModifiedSince := r.Header.Get("If-Modified-Since"); ifModifiedSince != "" {
		req.Header.Set("If-Modified-Since", ifModifiedSince)
	}

	// Send the request
	resp, err := config.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error fetching from upstream: %w", err)
	}

	return resp, nil
}

// updateCache updates both the content cache and the header cache
func updateCache(config ServerConfig, path string, body []byte, lastModified time.Time, headers http.Header) {
	// Update content cache
	err := config.Cache.Put(path, bytes.NewReader(body), int64(len(body)), lastModified)
	if err != nil {
		logging.Error("Error storing in cache: %v", err)
		// Continue even if caching fails
	} else if config.LogRequests {
		logging.Info("Stored in cache: %s (%d bytes)", path, len(body))
	}

	// Store original headers from upstream server in header cache
	err = config.HeaderCache.PutHeaders(path, headers)
	if err != nil {
		logging.Error("Error storing headers in cache: %v", err)
		// Continue even if header caching fails
	}
}

// respondWithContent sends the response to the client
func respondWithContent(w http.ResponseWriter, r *http.Request, headers http.Header, body []byte, contentLength int64) {
	// Set allowed response headers from upstream or cache
	filterAndSetHeaders(w, headers)

	// Set content length only if not already present
	if w.Header().Get("Content-Length") == "" {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", contentLength))
	}

	// Write status code and body (if not a HEAD request)
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, err := w.Write(body)
		if err != nil {
			logging.Error("Error writing response body: %v", err)
		}
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
	req.Header.Set("User-Agent", "Go-APT-Cache/1.0")

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
	if useIfModifiedSince && fileType == utils.TypeFrequentlyChanging {
		validationKey := fmt.Sprintf(validationCacheKeyFormat, r.URL.Path)
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
	}

	// If we reach here, serve the cached content

	// Convert io.ReadCloser to []byte
	bodyBytes, err := io.ReadAll(content)
	if err != nil {
		logging.Error("Error reading cached content: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return true // Return true as we handled the error
	}

	// Use cached headers exactly as they were stored, but only allowed ones
	filterAndSetHeaders(w, cachedHeaders)

	// Write status code and body (if not a HEAD request)
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, err := w.Write(bodyBytes)
		if err != nil {
			logging.Error("Error writing response body: %v", err)
		}
	}
	return true
}

// handleCacheMiss handles a cache miss, fetching the resource from the upstream server
func handleCacheMiss(w http.ResponseWriter, r *http.Request, config ServerConfig, useIfModifiedSince bool) {
	path := r.URL.Path

	// Acquire lock for this resource
	acquired, ch := acquireLock(path)
	if !acquired {
		// Wait for the other request to finish fetching
		<-ch

		// Check if the resource is now in cache
		content, contentLength, lastModified, err := config.Cache.Get(path)
		if err == nil {
			// Another request has fetched this resource
			if config.LogRequests {
				logging.Info("Resource was fetched by another request: %s", path)
			}
			handleCacheHit(w, r, config, content, contentLength, lastModified, useIfModifiedSince)
			return
		}

		// If still not in cache, acquire the lock and fetch it
		if !reacquireLock(path) {
			// This should not happen, but handle it gracefully
			logging.Error("Failed to acquire lock after waiting: %s", path)
			http.Error(w, "Server busy, please try again", http.StatusServiceUnavailable)
			return
		}
	}

	// We've acquired the lock, make sure to release it when done
	defer releaseLock(path)

	remotePath := getRemotePath(config, path)
	// Ensure we don't have double slashes in the URL
	upstreamURL := fmt.Sprintf("%s%s", config.UpstreamURL, remotePath)

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
		validationKey := fmt.Sprintf(validationCacheKeyFormat, path)
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
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body) // Forward the response body from the upstream server
		return
	}

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		logging.Error("Error reading response from upstream: %v", err)
		return
	}

	// Get last modified time
	lastModifiedTime := time.Now()
	if lastModifiedHeader := resp.Header.Get("Last-Modified"); lastModifiedHeader != "" {
		if parsedTime, err := time.Parse(http.TimeFormat, lastModifiedHeader); err == nil {
			lastModifiedTime = parsedTime
		}
	}

	// Update cache
	updateCache(config, path, body, lastModifiedTime, resp.Header)

	// Respond to the client
	respondWithContent(w, r, resp.Header, body, int64(len(body)))
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
			validationKey := fmt.Sprintf(validationCacheKeyFormat, r.URL.Path)
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
