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

var requestLock = struct {
	sync.RWMutex
	inProgress map[string]*cacheRequest
}{inProgress: make(map[string]*cacheRequest)}

type cacheRequest struct {
	done chan struct{}
}

var allowedResponseHeaders = map[string]bool{
	"Content-Type":   true,
	"Date":           true,
	"Etag":           true,
	"Last-Modified":  true,
	"Content-Length": true,
}

var clientCache = struct {
	sync.RWMutex
	clients map[int]*http.Client
}{clients: make(map[int]*http.Client)}

const defaultClientTimeout = 120
const defaultUserAgent = "Debian APT-HTTP/1.3 (2.2.4)"

func filterAndSetHeaders(w http.ResponseWriter, headers http.Header) {
	for header, values := range headers {
		if allowedResponseHeaders[http.CanonicalHeaderKey(header)] {
			for _, value := range values {
				w.Header().Add(header, value)
			}
		}
	}
}

func acquireLock(path string) bool {
	requestLock.Lock()
	defer requestLock.Unlock()

	if _, exists := requestLock.inProgress[path]; exists {
		return false
	}
	req := &cacheRequest{done: make(chan struct{})}
	requestLock.inProgress[path] = req
	return true
}

func releaseLock(path string) {
	requestLock.Lock()
	defer requestLock.Unlock()

	if req, exists := requestLock.inProgress[path]; exists {
		close(req.done)
		delete(requestLock.inProgress, path)
	}
}

func validateRequest(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return false
	}

	if r.URL.RawQuery != "" {
		http.Error(w, "Query parameters are not allowed", http.StatusForbidden)
		return false
	}

	return true
}

func getClient(config ServerConfig) *http.Client {
	if config.Client != nil {
		return config.Client
	}

	timeout := defaultClientTimeout

	clientCache.RLock()
	client, exists := clientCache.clients[timeout]
	clientCache.RUnlock()

	if exists {
		return client
	}

	client = utils.CreateHTTPClient(timeout)

	clientCache.Lock()
	clientCache.clients[timeout] = client
	clientCache.Unlock()

	return client
}

func getRemotePath(config ServerConfig, localPath string) string {
	// Normalize path by removing multiple slashes and ensuring consistent format
	normalizedPath := strings.Join(strings.FieldsFunc(localPath, func(r rune) bool {
		return r == '/'
	}), "/")

	// Remove repository prefix
	remotePath := strings.TrimPrefix(normalizedPath, strings.Trim(config.LocalPath, "/"))
	remotePath = strings.TrimPrefix(remotePath, "/")

	return remotePath
}

func getCacheKey(config ServerConfig, localPath string) string {
	// Get repository prefix
	repoPrefix := strings.Trim(config.LocalPath, "/")
	if repoPrefix == "" {
		repoPrefix = "root"
	}

	// Get remote path without repository prefix
	remotePath := getRemotePath(config, localPath)

	// Combine them ensuring single slash between parts
	return repoPrefix + "/" + remotePath
}

func updateCache(config ServerConfig, path string, body []byte, lastModified time.Time, headers http.Header) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	errChan := make(chan error, 2)

	go func() {
		defer wg.Done()
		logging.Debug("Cache update: Storing headers for %s", path)
		if err := config.HeaderCache.PutHeaders(path, headers); err != nil {
			logging.Error("Cache update: Error storing headers - %v", err)
			errChan <- fmt.Errorf("header error: %w", err)
			return
		}
		logging.Debug("Cache update: Headers stored successfully for %s", path)
	}()

	go func() {
		defer wg.Done()
		logging.Debug("Cache update: Storing content for %s (%d bytes)", path, len(body))
		if len(body) > 0 {
			if err := config.Cache.Put(path, bytes.NewReader(body), int64(len(body)), lastModified); err != nil {
				logging.Error("Cache update: Error storing content - %v", err)
				errChan <- fmt.Errorf("content error: %w", err)
				return
			}
			logging.Debug("Cache update: Content stored successfully for %s", path)
		} else {
			err := fmt.Errorf("empty body received for %s", path)
			logging.Error("Cache update: %v", err)
			errChan <- err
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		select {
		case err := <-errChan:
			logging.Error("Cache update: Error during update - %v", err)
			_ = config.HeaderCache.PutHeaders(path, http.Header{})
			if delErr := config.Cache.Put(path, bytes.NewReader([]byte{}), 0, time.Time{}); delErr != nil {
				logging.Error("Cache update: Failed to clear cache - %v", delErr)
			}

		default:
			if config.LogRequests {
				logging.Info("Cache: Stored headers for %s", path)
				logging.Info("Cache: Stored content for %s (%d bytes)", path, len(body))
			}
		}
	case <-ctx.Done():
		logging.Error("Cache update: Timed out for %s", path)
		_ = config.HeaderCache.PutHeaders(path, http.Header{})
		if delErr := config.Cache.Put(path, bytes.NewReader([]byte{}), 0, time.Time{}); delErr != nil {
			logging.Error("Cache update: Failed to clear cache - %v", delErr)
		}
	}
}

func checkAndHandleIfModifiedSince(w http.ResponseWriter, r *http.Request, lastModifiedStr string, lastModifiedTime time.Time, config ServerConfig) bool {
	ifModifiedSince := r.Header.Get("If-Modified-Since")
	if ifModifiedSince == "" {
		return false
	}

	ifModifiedSinceTime, err := time.Parse(http.TimeFormat, ifModifiedSince)
	if err != nil {
		if config.LogRequests {
			logging.Warning("Failed to parse If-Modified-Since header: %s, error: %v", ifModifiedSince, err)
		}
		return false
	}
	var lastModifiedTimeToCheck time.Time

	if lastModifiedStr != "" {
		lastModifiedTimeToCheck, err = time.Parse(http.TimeFormat, lastModifiedStr)
		if err != nil {
			lastModifiedTimeToCheck = lastModifiedTime
		}
	} else {
		lastModifiedTimeToCheck = lastModifiedTime
	}

	if !lastModifiedTimeToCheck.After(ifModifiedSinceTime) {
		sendNotModified(w, config, r)
		return true
	}

	return false
}

func validateWithUpstream(config ServerConfig, r *http.Request, cachedHeaders http.Header, cacheKey string) (bool, error) {
	remotePath := getRemotePath(config, r.URL.Path)
	upstreamURL := fmt.Sprintf("%s%s", config.UpstreamURL, remotePath)
	req, err := http.NewRequest(http.MethodHead, upstreamURL, nil)
	if err != nil {
		return false, fmt.Errorf("error creating HEAD request for validation: %w", err)
	}

	lastModifiedStr := cachedHeaders.Get("Last-Modified")
	if lastModifiedStr != "" {
		req.Header.Set("If-Modified-Since", lastModifiedStr)
	}
	etag := cachedHeaders.Get("ETag")
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	req.Header.Set("User-Agent", defaultUserAgent)

	logging.Debug("Validation: Checking %s", r.URL.Path)
	logging.Debug("Validation: Upstream URL=%s", upstreamURL)

	if lastModifiedStr != "" {
		logging.Debug("Validation: If-Modified-Since=%s", lastModifiedStr)
	}
	if etag != "" {
		logging.Debug("Validation: If-None-Match=%s", etag)
	}

	if config.LogRequests {
		logging.Info("Validation: Checking cached file with upstream: %s", r.URL.Path)
	}

	client := getClient(config)
	resp, err := client.Do(req)
	if err != nil {
		logging.Error("Validation: Error checking with upstream - %v", err)
		return false, fmt.Errorf("error checking with upstream: %w", err)
	}
	defer resp.Body.Close()

	logging.Debug("Validation: Upstream response status=%s", resp.Status)

	if resp.StatusCode == http.StatusNotModified {
		if config.LogRequests {
			logging.Info("Validation: Cache is valid according to upstream: %s", r.URL.Path)
		}
		mergedHeaders := mergeHeaders(cachedHeaders, resp.Header)
		config.HeaderCache.PutHeaders(cacheKey, mergedHeaders)
		return true, nil
	}

	if resp.StatusCode == http.StatusOK {
		mergedHeaders := mergeHeaders(cachedHeaders, resp.Header)
		config.HeaderCache.PutHeaders(cacheKey, mergedHeaders)
		return false, nil
	}

	return false, fmt.Errorf("unexpected upstream response: %d", resp.StatusCode)
}

func mergeHeaders(cachedHeaders, upstreamHeaders http.Header) http.Header {
	merged := make(http.Header)

	for k, v := range cachedHeaders {
		merged[k] = v
	}

	for k, v := range upstreamHeaders {
		if allowedResponseHeaders[http.CanonicalHeaderKey(k)] {
			merged[k] = v
		}
	}
	return merged
}

func handleCacheHit(w http.ResponseWriter, r *http.Request, config ServerConfig, content io.ReadCloser, lastModified time.Time, cacheKey string) bool {
	defer content.Close()

	cachedHeaders, headerErr := config.HeaderCache.GetHeaders(cacheKey)
	if headerErr != nil {
		logging.Error("No cached headers found for %s: %v", cacheKey, headerErr)
		return false

	}

	lastModifiedStr := cachedHeaders.Get("Last-Modified")

	if checkAndHandleIfModifiedSince(w, r, lastModifiedStr, lastModified, config) {
		return true
	}

	filterAndSetHeaders(w, cachedHeaders)

	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, err := io.Copy(w, content)
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") ||
				strings.Contains(err.Error(), "connection reset by peer") ||
				strings.Contains(err.Error(), "broken pipe") {
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

func handleCacheMiss(w http.ResponseWriter, r *http.Request, config ServerConfig, cacheKey string) {
	isFirstRequest := acquireLock(cacheKey)

	if isFirstRequest {
		defer releaseLock(cacheKey)

		remotePath := getRemotePath(config, r.URL.Path)
		upstreamURL := fmt.Sprintf("%s%s", config.UpstreamURL, remotePath)

		logging.Debug("handleCacheMiss: Fetching from upstream: %s → %s", cacheKey, upstreamURL)

		client := getClient(config)
		req, _ := http.NewRequest(r.Method, upstreamURL, nil)
		req.Header.Set("User-Agent", defaultUserAgent)

		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
			logging.Error("Error fetching content from upstream: %v", err)
			return
		}
		defer resp.Body.Close()

		if r.Method == http.MethodHead {
			filterAndSetHeaders(w, resp.Header)
			w.WriteHeader(resp.StatusCode)
			return
		}

		var buf bytes.Buffer
		mw := io.MultiWriter(w, &buf)

		lastModifiedTime := time.Now()
		if lastModifiedHeader := resp.Header.Get("Last-Modified"); lastModifiedHeader != "" {
			if parsedTime, err := time.Parse(http.TimeFormat, lastModifiedHeader); err == nil {
				lastModifiedTime = parsedTime
			}
		}

		filterAndSetHeaders(w, resp.Header)
		w.WriteHeader(resp.StatusCode)

		_, err = io.Copy(mw, resp.Body)
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") ||
				strings.Contains(err.Error(), "connection reset by peer") ||
				strings.Contains(err.Error(), "broken pipe") {
				if config.LogRequests {
					logging.Info("Client disconnected during download: %s", cacheKey)
				}
			} else {
				logging.Error("Error streaming response to client: %v", err)
			}
			return
		}

		logging.Debug("handleCacheMiss: Successfully fetched content for %s, storing in cache", cacheKey)
		go updateCache(config, cacheKey, buf.Bytes(), lastModifiedTime, resp.Header)
		validationKey := fmt.Sprintf("validation:%s", cacheKey)
		go config.ValidationCache.Put(validationKey, time.Now())
		logging.Debug("Cache validation: Updated key %s", validationKey)
		buf.Reset()

	} else {
		handleDirectUpstream(w, r, config)
	}
}

func handleDirectUpstream(w http.ResponseWriter, r *http.Request, config ServerConfig) {
	path := r.URL.Path
	remotePath := getRemotePath(config, path)
	upstreamURL := fmt.Sprintf("%s%s", config.UpstreamURL, remotePath)

	client := getClient(config)
	req, err := http.NewRequest(r.Method, upstreamURL, nil)
	if err != nil {
		http.Error(w, "Error creating request to upstream", http.StatusInternalServerError)
		logging.Error("Error creating request to upstream: %v", err)
		return
	}

	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
		logging.Error("Error fetching content from upstream: %v", err)
		return
	}
	defer resp.Body.Close()

	filterAndSetHeaders(w, resp.Header)
	if resp.StatusCode == http.StatusNotModified {
		sendNotModified(w, config, r)
		return
	}
	w.WriteHeader(resp.StatusCode)

	if r.Method != http.MethodHead {
		_, err = io.Copy(w, resp.Body)
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") ||
				strings.Contains(err.Error(), "connection reset by peer") ||
				strings.Contains(err.Error(), "broken pipe") {
				if config.LogRequests {
					logging.Info("Client disconnected during download: %s", path)
				}
			} else {
				logging.Error("Error streaming response to client: %v", err)
			}
		}
	}
}

func HandleRequest(config ServerConfig, useIfModifiedSince bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if config.LogRequests {
			logging.Info("Request: %s", r.URL.Path)
		}

		if !validateRequest(w, r) {
			return
		}

		cacheKey := getCacheKey(config, r.URL.Path)
		logging.Debug("Using cache key: %s for path: %s (repo: %s)",
			cacheKey, r.URL.Path, strings.Trim(config.LocalPath, "/"))

		validationKey := fmt.Sprintf("validation:%s", cacheKey)
		logging.Debug("Using validation key: %s", validationKey)

		fileType := utils.GetFilePatternType(r.URL.Path)
		if fileType == utils.TypeFrequentlyChanging {
			isValid, lastValidated := config.ValidationCache.Get(validationKey)
			if isValid {
				logging.Info("Validation cache: File %s is valid (last validated: %v)", validationKey, lastValidated)
				content, _, lastModified, err := config.Cache.Get(cacheKey)
				if err == nil {
					if handleCacheHit(w, r, config, content, lastModified, cacheKey) {
						return
					}
				}
			}
			if !isValid {
				cachedHeaders, headerErr := config.HeaderCache.GetHeaders(cacheKey)
				content, _, lastModified, err := config.Cache.Get(cacheKey)

				if headerErr == nil && err == nil {
					cacheIsValid, validationErr := validateWithUpstream(config, r, cachedHeaders, cacheKey)
					if validationErr != nil {
						logging.Error("Error validating with upstream: %v", validationErr)
						handleCacheMiss(w, r, config, cacheKey)
						return
					}
					if cacheIsValid {
						config.ValidationCache.Put(validationKey, time.Now())
						logging.Info("Validation cache: Updated for %s", validationKey)
						if handleCacheHit(w, r, config, content, lastModified, cacheKey) {
							return
						}
					} else {
						handleCacheMiss(w, r, config, cacheKey)
						return
					}
				} else {
					handleCacheMiss(w, r, config, cacheKey)
					return
				}
			} else {
				content, _, lastModified, err := config.Cache.Get(cacheKey)
				if err == nil {
					if handleCacheHit(w, r, config, content, lastModified, cacheKey) {
						return
					}
				} else {
					handleCacheMiss(w, r, config, cacheKey)
				}
			}

		} else {
			content, _, lastModified, err := config.Cache.Get(cacheKey)
			if err == nil {
				if handleCacheHit(w, r, config, content, lastModified, cacheKey) {
					return
				}
			} else {
				handleCacheMiss(w, r, config, cacheKey)
				return
			}
		}
	}
}

func HandleRelease(config ServerConfig) http.HandlerFunc {
	return HandleRequest(config, true)
}

func HandleCacheableRequest(config ServerConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fileType := utils.GetFilePatternType(r.URL.Path)
		HandleRequest(config, fileType == utils.TypeFrequentlyChanging)(w, r)
	}
}

func sendNotModified(w http.ResponseWriter, config ServerConfig, r *http.Request) {
	if config.LogRequests {
		logging.Info("Response: Not modified %s", r.URL.Path)
	}
	w.WriteHeader(http.StatusNotModified)
}
