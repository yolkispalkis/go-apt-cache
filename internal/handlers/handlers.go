package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
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

func acquireLock(path string) (bool, chan struct{}) {
	requestLock.Lock()
	defer requestLock.Unlock()

	if req, exists := requestLock.inProgress[path]; exists {
		return false, req.done
	}

	req := &cacheRequest{done: make(chan struct{})}
	requestLock.inProgress[path] = req
	return true, req.done
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
	remotePath := strings.TrimPrefix(localPath, config.LocalPath)
	remotePath = strings.TrimPrefix(remotePath, "/")

	return remotePath
}

func fetchFromUpstream(config ServerConfig, r *http.Request, upstreamURL string) (*http.Response, error) {
	req, err := http.NewRequest(r.Method, upstreamURL, nil)
	if err != nil {
		logging.Error("Error creating request to upstream: %v", err)
		return nil, err
	}

	fileType := utils.GetFilePatternType(r.URL.Path)
	if fileType == utils.TypeRarelyChanging {
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
		copyRelevantHeaders(req, r)
	}

	req.Header.Set("User-Agent", defaultUserAgent)

	if r.Header.Get("Accept-Encoding") != "" {
		req.Header.Set("Accept-Encoding", r.Header.Get("Accept-Encoding"))
	} else {
		req.Header.Set("Accept-Encoding", "gzip, deflate")
	}

	client := getClient(config)

	var timeout time.Duration
	if r.Method == http.MethodHead {
		timeout = 10 * time.Second
	} else {
		timeout = 60 * time.Second
	}

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := client.Do(req)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			logging.Error("Timeout fetching from upstream: %v", err)
			return nil, fmt.Errorf("timeout fetching from upstream: %w", err)
		}
		logging.Error("Error fetching from upstream: %v", err)
		return nil, err
	}
	return resp, nil
}

func copyRelevantHeaders(upstreamReq *http.Request, originalReq *http.Request) {
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

func updateCache(config ServerConfig, path string, body []byte, lastModified time.Time, headers http.Header) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	var headerErr, contentErr error

	go func() {
		defer wg.Done()
		headerErr = config.HeaderCache.PutHeaders(path, headers)
	}()

	go func() {
		defer wg.Done()
		if len(body) > 0 {
			contentErr = config.Cache.Put(path, bytes.NewReader(body), int64(len(body)), lastModified)
		} else {
			contentErr = fmt.Errorf("empty body received for %s", path)
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
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
	case <-ctx.Done():
		logging.Error("Cache update timed out for: %s", path)
	}
}

func sendNotModified(w http.ResponseWriter, config ServerConfig, r *http.Request) {
	if config.LogRequests {
		logging.Info("Resource not modified: %s", r.URL.Path)
	}
	w.WriteHeader(http.StatusNotModified)
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

func validateWithUpstream(config ServerConfig, r *http.Request, cachedHeaders http.Header, lastModified time.Time) (bool, error) {
	fileType := utils.GetFilePatternType(r.URL.Path)

	if fileType == utils.TypeRarelyChanging {
		if config.LogRequests {
			logging.Info("Skipping upstream validation for rarely changing file: %s", r.URL.Path)
		}
		return true, nil
	}

	remotePath := getRemotePath(config, r.URL.Path)
	upstreamURL := fmt.Sprintf("%s%s", config.UpstreamURL, remotePath)
	req, err := http.NewRequest(http.MethodHead, upstreamURL, nil)
	if err != nil {
		return false, fmt.Errorf("error creating HEAD request for validation: %w", err)
	}

	lastModifiedStr := cachedHeaders.Get("Last-Modified")
	if lastModifiedStr == "" {
		lastModifiedStr = lastModified.Format(http.TimeFormat)
	}
	req.Header.Set("If-Modified-Since", lastModifiedStr)
	req.Header.Set("User-Agent", defaultUserAgent)

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

	return false, nil
}

func handleCacheHit(w http.ResponseWriter, r *http.Request, config ServerConfig, content io.ReadCloser, contentLength int64, lastModified time.Time, useIfModifiedSince bool) bool {
	defer content.Close()

	cachedHeaders, headerErr := config.HeaderCache.GetHeaders(r.URL.Path)
	if headerErr != nil {
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

	lastModifiedStr := cachedHeaders.Get("Last-Modified")
	if useIfModifiedSince && checkAndHandleIfModifiedSince(w, r, lastModifiedStr, lastModified, config) {
		return true
	}

	fileType := utils.GetFilePatternType(r.URL.Path)

	if useIfModifiedSince && fileType == utils.TypeFrequentlyChanging {
		validationKey := fmt.Sprintf("validation:%s", r.URL.Path)
		isValid, _ := config.ValidationCache.Get(validationKey)

		if !isValid {
			cacheIsValid, err := validateWithUpstream(config, r, cachedHeaders, lastModified)
			if err != nil {
				logging.Error("Error validating with upstream: %v", err)
			} else {
				if cacheIsValid {
					config.ValidationCache.Put(validationKey, time.Now())

					if r.Header.Get("If-Modified-Since") != "" {
						if checkAndHandleIfModifiedSince(w, r, lastModifiedStr, lastModified, config) {
							return true
						}
					}
				} else {
					if config.LogRequests {
						logging.Info("Content modified on upstream, invalidating cache: %s", r.URL.Path)
					}
					return false
				}
			}
		} else {
			if r.Header.Get("If-Modified-Since") != "" {
				if checkAndHandleIfModifiedSince(w, r, lastModifiedStr, lastModified, config) {
					return true
				}
			}
		}
	} else if fileType == utils.TypeRarelyChanging {
		if r.Header.Get("If-Modified-Since") != "" {
			if checkAndHandleIfModifiedSince(w, r, lastModifiedStr, lastModified, config) {
				return true
			}
		}
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

func handleCacheMiss(w http.ResponseWriter, r *http.Request, config ServerConfig, useIfModifiedSince bool) {
	path := r.URL.Path

	acquired, done := acquireLock(path)
	if !acquired {
		if config.LogRequests {
			logging.Info("Waiting for another request to fetch: %s", path)
		}
		<-done

		content, contentLength, lastModified, err := config.Cache.Get(path)
		if err == nil {
			if config.LogRequests {
				logging.Info("Resource was fetched by another request: %s", path)
			}
			handleCacheHit(w, r, config, content, contentLength, lastModified, useIfModifiedSince)
			return
		}

		logging.Error("Resource not found in cache after waiting for another request: %s", path)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	defer releaseLock(path)

	remotePath := getRemotePath(config, path)
	upstreamURL := fmt.Sprintf("%s%s", config.UpstreamURL, remotePath)

	if r.Method == http.MethodHead {
		handleHeadRequest(w, r, config, upstreamURL, path)
		return
	}

	handleGetRequest(w, r, config, upstreamURL, path)
}

func handleHeadRequest(w http.ResponseWriter, r *http.Request, config ServerConfig, upstreamURL string, path string) {
	resp, err := fetchFromUpstream(config, r, upstreamURL)
	if err != nil {
		logging.Error("HEAD request failed, falling back to GET: %v", err)
		handleGetRequest(w, r, config, upstreamURL, path)
		return
	}
	defer resp.Body.Close()

	lastModifiedTime := time.Now()
	if lastModifiedHeader := resp.Header.Get("Last-Modified"); lastModifiedHeader != "" {
		if parsedTime, err := time.Parse(http.TimeFormat, lastModifiedHeader); err == nil {
			lastModifiedTime = parsedTime
		}
	}

	filterAndSetHeaders(w, resp.Header)
	w.WriteHeader(resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logging.Error("Error reading response from upstream for HEAD request: %v", err)
		return
	}

	go updateCache(config, path, body, lastModifiedTime, resp.Header)
}

func handleGetRequest(w http.ResponseWriter, r *http.Request, config ServerConfig, upstreamURL string, path string) {
	client := getClient(config)

	getReq, _ := http.NewRequest(http.MethodGet, upstreamURL, nil)
	copyRelevantHeaders(getReq, r)
	getReq.Header.Set("User-Agent", defaultUserAgent)

	getResp, err := client.Do(getReq)
	if err != nil {
		http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
		logging.Error("Error fetching content from upstream: %v", err)
		return
	}
	defer getResp.Body.Close()

	if getResp.StatusCode == http.StatusNotModified {
		if config.LogRequests {
			logging.Info("Upstream reports resource not modified: %s", path)
		}

		validationKey := fmt.Sprintf("validation:%s", path)
		config.ValidationCache.Put(validationKey, time.Now())
		if config.LogRequests {
			logging.Info("Stored validation result in cache for: %s", path)
		}

		sendNotModified(w, config, r)
		return
	}

	if getResp.StatusCode != http.StatusOK {
		logging.Error("Unexpected status code from upstream GET: %d, URL: %s", getResp.StatusCode, upstreamURL)

		filterAndSetHeaders(w, getResp.Header)

		w.WriteHeader(getResp.StatusCode)
		io.Copy(w, getResp.Body)
		return
	}

	lastModifiedTime := time.Now()
	if lastModifiedHeader := getResp.Header.Get("Last-Modified"); lastModifiedHeader != "" {
		if parsedTime, err := time.Parse(http.TimeFormat, lastModifiedHeader); err == nil {
			lastModifiedTime = parsedTime
		}
	}

	filterAndSetHeaders(w, getResp.Header)

	if getResp.ContentLength > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", getResp.ContentLength))
	}

	w.WriteHeader(http.StatusOK)

	var buf bytes.Buffer
	defer buf.Reset()

	mw := io.MultiWriter(w, &buf)

	_, err = io.Copy(mw, getResp.Body)
	if err != nil {
		if strings.Contains(err.Error(), "context canceled") ||
			strings.Contains(err.Error(), "connection reset by peer") ||
			strings.Contains(err.Error(), "broken pipe") {
			if config.LogRequests {
				logging.Info("Client disconnected during download: %s", path)
			}
			return
		} else {
			logging.Error("Error streaming response to client: %v", err)
		}
	}
	bufBytes := buf.Bytes()

	go updateCache(config, path, bufBytes, lastModifiedTime, getResp.Header)
}

func setBasicHeaders(w http.ResponseWriter, r *http.Request, _ http.Header, lastModified time.Time, useIfModifiedSince bool, config ServerConfig) {
	if strings.HasSuffix(r.URL.Path, "/") {
		w.Header().Set("Content-Type", "text/html")
	} else {
		if w.Header().Get("Content-Type") == "" {
			contentType := utils.GetContentType(r.URL.Path)
			if contentType != "" {
				w.Header().Set("Content-Type", contentType)
			} else {
				logging.Warning("Could not determine content type for: %s", r.URL.Path)
				w.Header().Set("Content-Type", "application/octet-stream")
			}
		}
	}
	w.Header().Set("Last-Modified", lastModified.Format(http.TimeFormat))

	if useIfModifiedSince && checkAndHandleIfModifiedSince(w, r, "", lastModified, config) {
		return
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

		content, contentLength, lastModified, err := config.Cache.Get(r.URL.Path)
		if err == nil {
			if config.LogRequests {
				logging.Info("Cache hit for: %s", r.URL.Path)
			}
			if handleCacheHit(w, r, config, content, contentLength, lastModified, useIfModifiedSince) {
				return
			}
		}

		handleCacheMiss(w, r, config, useIfModifiedSince)
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
