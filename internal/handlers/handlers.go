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
	OriginServer string
	Cache        storage.Cache
	HeaderCache  storage.HeaderCache
	LogRequests  bool
	Client       *http.Client // HTTP client for making requests to origin servers
}

// requestLock provides a mechanism to prevent concurrent requests for the same resource
// This helps prevent the "thundering herd" problem where multiple clients request the same
// uncached resource simultaneously
var requestLock = struct {
	sync.RWMutex
	inProgress map[string]chan struct{}
}{inProgress: make(map[string]chan struct{})}

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
		http.Error(w, "Forbidden", http.StatusForbidden)
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

// handleCacheHit handles a cache hit, returning true if the response was handled
func handleCacheHit(w http.ResponseWriter, r *http.Request, config ServerConfig, content io.ReadCloser, contentLength int64, lastModified time.Time, useIfModifiedSince bool) bool {
	defer content.Close()

	// Try to get cached headers
	cachedHeaders, headerErr := config.HeaderCache.GetHeaders(r.URL.Path)
	if headerErr == nil {
		// Check If-Modified-Since header from client request
		ifModifiedSince := r.Header.Get("If-Modified-Since")
		if useIfModifiedSince && ifModifiedSince != "" {
			if config.LogRequests {
				log.Printf("Checking If-Modified-Since: %s for path: %s", ifModifiedSince, r.URL.Path)
			}

			ifModifiedSinceTime, err := time.Parse(http.TimeFormat, ifModifiedSince)
			if err == nil {
				// Get Last-Modified from cached headers or use the file's lastModified
				lastModifiedStr := cachedHeaders.Get("Last-Modified")
				var lastModifiedTime time.Time

				if lastModifiedStr != "" {
					lastModifiedTime, err = time.Parse(http.TimeFormat, lastModifiedStr)
					if err != nil {
						lastModifiedTime = lastModified
					}
				} else {
					lastModifiedTime = lastModified
				}

				if config.LogRequests {
					log.Printf("Comparing - Last-Modified: %s, If-Modified-Since: %s for path: %s",
						lastModifiedTime.Format(http.TimeFormat),
						ifModifiedSinceTime.Format(http.TimeFormat),
						r.URL.Path)
				}

				if !lastModifiedTime.After(ifModifiedSinceTime) {
					// Resource not modified
					if config.LogRequests {
						log.Printf("Resource not modified (304) - Last-Modified is not after If-Modified-Since for path: %s", r.URL.Path)
					}
					w.WriteHeader(http.StatusNotModified)
					return true
				} else if config.LogRequests {
					log.Printf("Resource modified - Last-Modified is after If-Modified-Since for path: %s", r.URL.Path)
				}
			} else if config.LogRequests {
				log.Printf("Failed to parse If-Modified-Since header: %s, error: %v", ifModifiedSince, err)
			}
		}

		// Check with upstream server if our cache is still valid
		// Only do this for frequently changing files to reduce load on origin servers
		if useIfModifiedSince && shouldValidateWithOrigin(r.URL.Path) {
			originURL := fmt.Sprintf("%s%s", config.OriginServer, r.URL.Path)
			req, err := http.NewRequest(http.MethodHead, originURL, nil)
			if err == nil {
				// Use our cached Last-Modified as If-Modified-Since when checking upstream
				lastModifiedStr := cachedHeaders.Get("Last-Modified")
				if lastModifiedStr != "" {
					req.Header.Set("If-Modified-Since", lastModifiedStr)
					if config.LogRequests {
						log.Printf("Checking with upstream using If-Modified-Since: %s for path: %s", lastModifiedStr, r.URL.Path)
					}
				} else {
					formattedTime := lastModified.Format(http.TimeFormat)
					req.Header.Set("If-Modified-Since", formattedTime)
					if config.LogRequests {
						log.Printf("Checking with upstream using If-Modified-Since (from file time): %s for path: %s", formattedTime, r.URL.Path)
					}
				}

				// Add User-Agent header
				req.Header.Set("User-Agent", "Go-APT-Cache/1.0")

				// Check with upstream
				client := getClient(config)
				resp, err := client.Do(req)
				if err == nil {
					defer resp.Body.Close()

					if resp.StatusCode == http.StatusNotModified {
						// Our cache is still valid, use it
						if config.LogRequests {
							log.Printf("Upstream confirms cache is still valid (304) for: %s", r.URL.Path)
						}
					} else if resp.StatusCode == http.StatusOK {
						// Upstream has a newer version, fetch it
						log.Printf("Upstream has newer version (200) for: %s", r.URL.Path)

						// Acquire lock for this resource to prevent multiple concurrent fetches
						acquired, ch := acquireLock(r.URL.Path)
						if acquired {
							defer releaseLock(r.URL.Path)
						} else {
							<-ch
						}

						content, contentLength, cachedHeaders = fetchAndUpdateCache(config, r.URL.Path, originURL, client)
					} else {
						log.Printf("Unexpected status from upstream: %d for %s", resp.StatusCode, r.URL.Path)
					}
				} else {
					log.Printf("Error checking with upstream: %v for %s", err, r.URL.Path)
				}
			} else {
				log.Printf("Error creating HEAD request: %v", err)
			}
		}

		// Use cached headers
		for key, values := range cachedHeaders {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
	} else {
		// Fallback to basic headers if no cached headers
		setBasicHeaders(w, r, cachedHeaders, lastModified, useIfModifiedSince)
	}

	// Always set content length
	w.Header().Set("Content-Length", fmt.Sprintf("%d", contentLength))

	// If it's a HEAD request, don't send the body
	if r.Method == http.MethodHead {
		return true
	}

	// Copy content to response writer with proper error handling
	_, err := io.Copy(w, content)
	if err != nil {
		log.Printf("Error writing response: %v", err)
	}

	return true
}

// fetchAndUpdateCache fetches content from origin and updates the cache
func fetchAndUpdateCache(config ServerConfig, path string, originURL string, client *http.Client) (io.ReadCloser, int64, http.Header) {
	// Fetch the full content with a GET request
	getReq, err := http.NewRequest(http.MethodGet, originURL, nil)
	if err != nil {
		log.Printf("Error creating GET request: %v", err)
		return nil, 0, nil
	}

	getReq.Header.Set("User-Agent", "Go-APT-Cache/1.0")
	getResp, err := client.Do(getReq)
	if err != nil {
		log.Printf("Error fetching from origin: %v", err)
		return nil, 0, nil
	}
	defer getResp.Body.Close()

	// Read the entire response body
	bodyBytes, err := io.ReadAll(getResp.Body)
	if err != nil {
		log.Printf("Error reading response: %v", err)
		return nil, 0, nil
	}

	// Validate file size if Content-Length header is present
	contentLength := getResp.ContentLength
	actualSize := int64(len(bodyBytes))
	if contentLength > 0 && contentLength != actualSize {
		log.Printf("File size validation failed for %s: expected %d bytes, got %d bytes", path, contentLength, actualSize)
		return io.NopCloser(bytes.NewReader(bodyBytes)), actualSize, getResp.Header
	}

	// Get last modified time
	lastModifiedTime := time.Now()
	if lastModifiedHeader := getResp.Header.Get("Last-Modified"); lastModifiedHeader != "" {
		if parsedTime, err := time.Parse(http.TimeFormat, lastModifiedHeader); err == nil {
			lastModifiedTime = parsedTime
		}
	}

	// Update cache
	cacheErr := config.Cache.Put(path, bytes.NewReader(bodyBytes), int64(len(bodyBytes)), lastModifiedTime)
	if cacheErr != nil {
		log.Printf("Error updating cache: %v", cacheErr)
	} else {
		log.Printf("Successfully updated cache: %s", path)
	}

	// Update header cache
	headerErr := config.HeaderCache.PutHeaders(path, getResp.Header)
	if headerErr != nil {
		log.Printf("Error updating headers: %v", headerErr)
	}

	// Return the new content and headers
	return io.NopCloser(bytes.NewReader(bodyBytes)), int64(len(bodyBytes)), getResp.Header
}

// setBasicHeaders sets basic headers when cached headers are not available
func setBasicHeaders(w http.ResponseWriter, r *http.Request, cachedHeaders http.Header, lastModified time.Time, useIfModifiedSince bool) {
	// For directory URLs, always use text/html
	if strings.HasSuffix(r.URL.Path, "/") {
		w.Header().Set("Content-Type", "text/html")
	} else {
		// Try to get Content-Type from cached headers first
		contentType := ""
		if cachedHeaders != nil {
			contentType = cachedHeaders.Get("Content-Type")
		}
		// If Content-Type is not in cached headers, determine it from file extension
		if contentType == "" {
			contentType = utils.GetContentType(r.URL.Path)
		}
		w.Header().Set("Content-Type", contentType)
	}
	w.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))

	// Check If-Modified-Since header only if we should use it for this file type
	if useIfModifiedSince {
		ifModifiedSince := r.Header.Get("If-Modified-Since")
		if ifModifiedSince != "" {
			ifModifiedSinceTime, err := time.Parse(http.TimeFormat, ifModifiedSince)
			if err == nil && !lastModified.After(ifModifiedSinceTime) {
				// Resource not modified
				w.WriteHeader(http.StatusNotModified)
			}
		}
	}
}

// handleCacheMiss handles a cache miss, fetching the resource from the origin server
func handleCacheMiss(w http.ResponseWriter, r *http.Request, config ServerConfig, useIfModifiedSince bool) {
	path := r.URL.Path

	// Check if this resource is already being fetched by another request
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
		acquired, ch = acquireLock(path)
		if !acquired {
			// This should not happen, but handle it gracefully
			http.Error(w, "Server busy, please try again", http.StatusServiceUnavailable)
			return
		}
	}

	// We've acquired the lock, make sure to release it when done
	defer releaseLock(path)

	originURL := fmt.Sprintf("%s%s", config.OriginServer, path)
	if config.LogRequests {
		log.Printf("Cache miss, fetching from origin: %s", originURL)
	}

	// Create request to origin server
	req, err := http.NewRequest(r.Method, originURL, nil)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log.Printf("Error creating request to origin: %v", err)
		return
	}

	// Copy relevant headers from client request to origin request
	// Add User-Agent header
	req.Header.Set("User-Agent", "Go-APT-Cache/1.0")

	// Add If-Modified-Since header if present in client request and we should use it
	if useIfModifiedSince {
		if ifModifiedSince := r.Header.Get("If-Modified-Since"); ifModifiedSince != "" {
			req.Header.Set("If-Modified-Since", ifModifiedSince)
			if config.LogRequests {
				log.Printf("Cache miss: Forwarding If-Modified-Since: %s to origin for path: %s", ifModifiedSince, path)
			}
		}
	}

	// Make request to origin server with timeout
	client := getClient(config)
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
		log.Printf("Error fetching from origin: %v", err)
		return
	}
	defer resp.Body.Close()

	// Handle response from origin server
	if resp.StatusCode == http.StatusNotModified {
		// Resource not modified
		if config.LogRequests {
			log.Printf("Origin reports resource not modified (304) for path: %s", path)
		}
		w.WriteHeader(http.StatusNotModified)
		return
	}

	if resp.StatusCode != http.StatusOK {
		// Forward error status from origin
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log.Printf("Error reading response from origin: %v", err)
		return
	}

	// Store in cache
	lastModifiedTime := time.Now()
	if lastModifiedHeader := resp.Header.Get("Last-Modified"); lastModifiedHeader != "" {
		if parsedTime, err := time.Parse(http.TimeFormat, lastModifiedHeader); err == nil {
			lastModifiedTime = parsedTime
		}
	}

	err = config.Cache.Put(path, bytes.NewReader(body), int64(len(body)), lastModifiedTime)
	if err != nil {
		log.Printf("Error storing in cache: %v", err)
		// Continue even if caching fails
	} else if config.LogRequests {
		log.Printf("Stored in cache: %s (%d bytes)", path, len(body))
	}

	// Store headers in header cache
	err = config.HeaderCache.PutHeaders(path, resp.Header)
	if err != nil {
		log.Printf("Error storing headers in cache: %v", err)
		// Continue even if header caching fails
	}

	// Set response headers
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Set content type if not already set
	if w.Header().Get("Content-Type") == "" {
		contentType := utils.GetContentType(path)
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
	}

	// Write response
	w.WriteHeader(resp.StatusCode)
	if r.Method != http.MethodHead {
		w.Write(body)
	}
}

// shouldUseIfModifiedSince determines if a file should use If-Modified-Since logic
func shouldUseIfModifiedSince(path string) bool {
	// Use the file pattern type to determine if we should use If-Modified-Since
	patternType := utils.GetFilePatternType(path)
	return patternType == utils.FrequentlyChanging || patternType == utils.CriticalMetadata
}

// shouldValidateWithOrigin determines if we should check with the origin server
// to validate if our cached copy is still valid
func shouldValidateWithOrigin(path string) bool {
	// Only validate critical metadata files with origin
	return utils.GetFilePatternType(path) == utils.CriticalMetadata
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

		// Try to get from cache first
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

		// Cache miss
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
