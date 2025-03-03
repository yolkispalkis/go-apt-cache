package handlers

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/storage"
	"github.com/yolkispalkis/go-apt-cache/internal/utils"
)

// ServerConfig holds the configuration for the APT mirror server
type ServerConfig struct {
	OriginServer    string
	Cache           storage.Cache
	HeaderCache     storage.HeaderCache
	ValidationCache storage.ValidationCache
	LogRequests     bool
	Client          *http.Client // HTTP client for making requests to origin servers
	LocalPath       string       // Local path prefix for URL mapping
}

// requestLock provides a mechanism to prevent concurrent requests for the same resource
// This helps prevent the "thundering herd" problem where multiple clients request the same
// uncached resource simultaneously
var requestLock = struct {
	sync.RWMutex
	inProgress map[string]chan struct{}
}{inProgress: make(map[string]chan struct{})}

// validationCacheKeyFormat is the format string for validation cache keys.
const validationCacheKeyFormat = "validation:%s"

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

// getOriginPath converts local path to origin path
func getOriginPath(config ServerConfig, localPath string) string {
	// Remove local path prefix
	originPath := strings.TrimPrefix(localPath, config.LocalPath)
	// Ensure path starts with /
	if !strings.HasPrefix(originPath, "/") {
		originPath = "/" + originPath
	}
	return originPath
}

// fetchFromOrigin fetches content from the origin server
func fetchFromOrigin(config ServerConfig, r *http.Request, originURL string) (*http.Response, error) {
	if config.LogRequests {
		log.Printf("Fetching from origin: %s", originURL)
	}

	req, err := http.NewRequest(r.Method, originURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request to origin: %w", err)
	}

	// Copy relevant headers from the client request to the origin request
	req.Header.Set("User-Agent", "Go-APT-Cache/1.0")
	if ifModifiedSince := r.Header.Get("If-Modified-Since"); ifModifiedSince != "" {
		req.Header.Set("If-Modified-Since", ifModifiedSince)
	}

	client := getClient(config)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error fetching from origin: %w", err)
	}
	// Note: Caller is responsible for closing resp.Body

	return resp, nil
}

// updateCache updates both the content cache and the header cache
func updateCache(config ServerConfig, path string, body []byte, lastModified time.Time, headers http.Header) {
	// Update content cache
	err := config.Cache.Put(path, bytes.NewReader(body), int64(len(body)), lastModified)
	if err != nil {
		log.Printf("Error storing in cache: %v", err)
		// Continue even if caching fails
	} else if config.LogRequests {
		log.Printf("Stored in cache: %s (%d bytes)", path, len(body))
	}

	// Update header cache
	err = config.HeaderCache.PutHeaders(path, headers)
	if err != nil {
		log.Printf("Error storing headers in cache: %v", err)
		// Continue even if header caching fails
	}
}

// respondWithContent sends the response to the client
func respondWithContent(w http.ResponseWriter, r *http.Request, headers http.Header, body []byte, contentLength int64) {
	// Set response headers from origin or cache
	for key, values := range headers {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Always set content length
	w.Header().Set("Content-Length", fmt.Sprintf("%d", contentLength))

	// Write status code and body (if not a HEAD request)
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, err := w.Write(body)
		if err != nil {
			log.Printf("Error writing response body: %v", err)
		}
	}
}

// sendNotModified sends a 304 Not Modified response.
func sendNotModified(w http.ResponseWriter, config ServerConfig, r *http.Request) {
	if config.LogRequests {
		log.Printf("Resource not modified: %s", r.URL.Path)
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
			log.Printf("Failed to parse If-Modified-Since header: %s, error: %v", ifModifiedSince, err)
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

// validateWithOrigin checks with the origin server if the cached copy is still valid
func validateWithOrigin(config ServerConfig, r *http.Request, cachedHeaders http.Header, lastModified time.Time) (bool, error) {
	originPath := getOriginPath(config, r.URL.Path)
	originURL := fmt.Sprintf("%s%s", config.OriginServer, originPath)
	req, err := http.NewRequest(http.MethodHead, originURL, nil)
	if err != nil {
		return false, fmt.Errorf("error creating HEAD request for validation: %w", err)
	}

	// Use our cached Last-Modified as If-Modified-Since when checking upstream
	lastModifiedStr := cachedHeaders.Get("Last-Modified")
	if lastModifiedStr == "" {
		lastModifiedStr = lastModified.Format(http.TimeFormat)
	}
	req.Header.Set("If-Modified-Since", lastModifiedStr)
	req.Header.Set("User-Agent", "Go-APT-Cache/1.0")

	if config.LogRequests {
		log.Printf("Validating cached file with upstream: %s", r.URL.Path)
	}

	client := getClient(config)
	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("error checking with upstream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		if config.LogRequests {
			log.Printf("Upstream confirms cache is still valid: %s", r.URL.Path)
		}
		return true, nil
	} else if resp.StatusCode != http.StatusOK {
		log.Printf("Unexpected status from upstream during validation: %d for %s", resp.StatusCode, r.URL.Path)
	}
	return false, nil
}

// handleCacheHit handles a cache hit, returning true if the response was handled
func handleCacheHit(w http.ResponseWriter, r *http.Request, config ServerConfig, content io.ReadCloser, contentLength int64, lastModified time.Time, useIfModifiedSince bool) bool {
	defer content.Close()

	cachedHeaders, headerErr := config.HeaderCache.GetHeaders(r.URL.Path)
	if headerErr != nil {
		// Fallback to basic headers if no cached headers
		log.Printf("No cached headers found for %s: %v", r.URL.Path, headerErr)
		setBasicHeaders(w, r, nil, lastModified, useIfModifiedSince, config)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", contentLength))

		if r.Method == http.MethodHead {
			return true
		}
		_, err := io.Copy(w, content)
		if err != nil {
			log.Printf("Error writing response: %v", err)
		}
		return true
	}

	// Check If-Modified-Since
	lastModifiedStr := cachedHeaders.Get("Last-Modified")
	if useIfModifiedSince && checkAndHandleIfModifiedSince(w, r, lastModifiedStr, lastModified, config) {
		return true
	}

	// Validate with origin if needed
	if useIfModifiedSince && shouldValidateWithOrigin(r.URL.Path) {
		validationKey := fmt.Sprintf(validationCacheKeyFormat, r.URL.Path)
		isValid, _ := config.ValidationCache.Get(validationKey)
		if !isValid {
			cacheIsValid, err := validateWithOrigin(config, r, cachedHeaders, lastModified)
			if err != nil {
				log.Printf("Error validating with origin: %v", err)
				// If validation fails, we still serve the cached content, but log the error
			} else if cacheIsValid {
				config.ValidationCache.Put(validationKey, time.Now())
				sendNotModified(w, config, r)
				return true
			}
		} else {
			// We have a recent valid result, no need to check with origin
			if config.LogRequests {
				log.Printf("Using cached validation result for: %s", r.URL.Path)
			}
			sendNotModified(w, config, r)
			return true
		}
	}

	// If we reach here, we either didn't need to validate, or validation failed (or we skipped it due to an error).
	// In any case, we serve the cached content

	// Convert io.ReadCloser to []byte
	bodyBytes, err := io.ReadAll(content)
	if err != nil {
		log.Printf("Error reading cached content: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return true // Return true as we handled the error
	}

	respondWithContent(w, r, cachedHeaders, bodyBytes, int64(len(bodyBytes)))
	return true
}

// handleCacheMiss handles a cache miss, fetching the resource from the origin server
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
				log.Printf("Resource was fetched by another request: %s", path)
			}
			handleCacheHit(w, r, config, content, contentLength, lastModified, useIfModifiedSince)
			return
		}

		// If still not in cache, acquire the lock and fetch it
		if !reacquireLock(path) {
			// This should not happen, but handle it gracefully
			log.Printf("Failed to acquire lock after waiting: %s", path)
			http.Error(w, "Server busy, please try again", http.StatusServiceUnavailable)
			return
		}
	}

	// We've acquired the lock, make sure to release it when done
	defer releaseLock(path)

	originPath := getOriginPath(config, path)
	originURL := fmt.Sprintf("%s%s", config.OriginServer, originPath)

	// Fetch from origin
	resp, err := fetchFromOrigin(config, r, originURL)
	if err != nil {
		http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
		log.Printf("Error fetching from origin: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		// Resource not modified (This should only happen if If-Modified-Since was sent)
		if config.LogRequests {
			log.Printf("Origin reports resource not modified: %s", path)
		}

		// Store validation result in validation cache
		validationKey := fmt.Sprintf(validationCacheKeyFormat, path)
		config.ValidationCache.Put(validationKey, time.Now())
		if config.LogRequests {
			log.Printf("Stored validation result in cache for: %s", path)
		}

		sendNotModified(w, config, r)
		return
	}

	if resp.StatusCode != http.StatusOK {
		// Forward error status from origin
		log.Printf("Unexpected status code from origin: %d, URL: %s", resp.StatusCode, originURL)
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body) // Forward the response body from the origin server
		return
	}

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log.Printf("Error reading response from origin: %v", err)
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

// shouldUseIfModifiedSince determines if a file should use If-Modified-Since logic
func shouldUseIfModifiedSince(path string) bool {
	// Use the file pattern type to determine if we should use If-Modified-Since
	patternType := utils.GetFilePatternType(path)
	return patternType == utils.TypeFrequentlyChanging
}

// shouldValidateWithOrigin determines if we should check with the origin server
// to validate if our cached copy is still valid
func shouldValidateWithOrigin(path string) bool {
	// Validate Release files and directories
	return strings.Contains(path, "InRelease") ||
		strings.Contains(path, "Release") ||
		strings.HasSuffix(path, "/")
}

// HandleRequest is a common handler for all types of requests
func HandleRequest(config ServerConfig, useIfModifiedSince bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if config.LogRequests {
			log.Printf("Request: %s", r.URL.Path)
		}

		if !validateRequest(w, r) {
			return
		}

		// Check if we have a recent validation result in the validation cache first
		if useIfModifiedSince {
			validationKey := fmt.Sprintf(validationCacheKeyFormat, r.URL.Path)
			isValid, _ := config.ValidationCache.Get(validationKey)
			if isValid {
				if config.LogRequests {
					log.Printf("Using cached validation result for: %s", r.URL.Path)
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
				log.Printf("Cache hit for: %s", r.URL.Path)
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
func HandleCacheableRequest(config ServerConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Determine if this file should use If-Modified-Since based on its path
		useIfModifiedSince := shouldUseIfModifiedSince(r.URL.Path)
		HandleRequest(config, useIfModifiedSince)(w, r)
	}
}
