// internal/fetch/fetch.go
package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/yolkispalkis/go-apt-cache/internal/logging"
)

var (
	ErrNotFound             = errors.New("upstream resource not found (404)")
	ErrUpstreamNotModified  = errors.New("upstream resource not modified (304)")
	ErrUpstreamError        = errors.New("upstream server error")
	ErrRequestConfiguration = errors.New("invalid request configuration")
)

// Result holds the outcome of a successful fetch operation.
type Result struct {
	Body       io.ReadCloser // Stream of the response body
	Header     http.Header   // Relevant headers from the upstream response
	Size       int64         // Content length, -1 if unknown
	ModTime    time.Time     // Last-Modified time from header
	StatusCode int           // HTTP status code from upstream
}

// Coordinator manages concurrent fetches to upstream servers.
type Coordinator struct {
	httpClient     *http.Client
	fetchGroup     singleflight.Group
	requestTimeout time.Duration
}

// NewCoordinator creates a new fetch coordinator.
func NewCoordinator(requestTimeout time.Duration, maxConcurrent int) *Coordinator {
	// Configure transport for connection pooling and keep-alives
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment, // Respect standard proxy env vars
		MaxIdleConns:          maxConcurrent * 2,         // Allow more idle conns than concurrent fetches
		MaxIdleConnsPerHost:   maxConcurrent,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true, // Enable HTTP/2
		// Consider adding DialContext timeouts if needed
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   requestTimeout, // Overall request timeout
		// Do not follow redirects automatically, APT clients handle them? Verify this.
		// If redirects need handling, do it manually to control cache keys.
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			logging.Warn("Upstream redirect detected for %s. Redirects are currently not followed.", req.URL)
			return http.ErrUseLastResponse // Stop redirect processing
		},
	}

	// Log proxy settings if detected
	proxyURL, err := http.ProxyFromEnvironment(&http.Request{URL: &url.URL{Scheme: "http", Host: "example.com"}}) // Test URL
	if err == nil && proxyURL != nil {
		logging.Info("Using system proxy: %s", proxyURL)
	} else if err != nil {
		logging.Warn("Error checking proxy settings: %v", err)
	} else {
		logging.Info("No system proxy configured or detected.")
	}

	return &Coordinator{
		httpClient:     client,
		requestTimeout: requestTimeout,
	}
}

// Fetch retrieves a resource, coordinating concurrent requests for the same key.
// It handles making the actual HTTP request and interpreting the response.
// `clientHeader` contains headers from the *original* client request (e.g., If-Modified-Since).
func (c *Coordinator) Fetch(ctx context.Context, cacheKey, upstreamURL string, clientHeader http.Header) (*Result, error) {
	// Use singleflight to ensure only one concurrent fetch per unique cacheKey
	// The result needs careful handling because io.ReadCloser cannot be shared directly.
	// We fetch the metadata and potentially the body, then return it.
	// If singleflight returns shared result, subsequent callers might need to re-fetch body if first one consumed it.
	// Alternative: Singleflight function streams to cache AND returns metadata. Callers then read from cache.

	// Let's try returning the response directly, assuming callers handle body closing.
	// The singleflight function MUST return an interface{} and error.
	v, err, _ := c.fetchGroup.Do(cacheKey, func() (interface{}, error) {
		return c.doFetch(ctx, upstreamURL, clientHeader)
	})

	if err != nil {
		// Error during the actual fetch attempt
		return nil, err
	}

	result, ok := v.(*Result)
	if !ok {
		// Should not happen if doFetch returns correctly
		return nil, fmt.Errorf("internal error: unexpected type returned from singleflight group")
	}

	// Important: If the body is shared via singleflight, only the *first* caller
	// gets the original ReadCloser. Subsequent callers get a closed body or error.
	// This approach is likely flawed for sharing the raw response body.

	// --- Revised Singleflight Approach ---
	// The singleflight function fetches metadata and *writes to cache*.
	// It returns metadata (size, modtime) or an error.
	// All callers (original and waiting) then attempt to read from cache.
	// This decouples fetching from serving the client directly through singleflight.
	// This seems overly complex compared to the handler managing the TeeReader.

	// --- Let's stick to the simpler model ---
	// The handler calls Fetch. Fetch uses singleflight. The *first* flight performs
	// the HTTP request and returns the raw response (*Result). The handler then
	// uses TeeReader to stream to client and cache. Subsequent singleflight callers
	// will get the *same* *Result pointer*. This means they CANNOT consume the Body.
	// This still seems wrong.

	// --- Final Approach Attempt ---
	// Singleflight key = cacheKey.
	// Function = `doFetch` which returns `*Result` or error.
	// The `*Result` contains the `http.Response.Body`.
	// The *handler* is responsible for recognizing if it was the leader or a follower.
	// How? Singleflight doesn't easily expose this.

	// Maybe singleflight isn't the right tool if we want to stream directly from the
	// first successful fetch to multiple waiting clients *and* the cache simultaneously.
	// Let's abandon singleflight for now and rely on the handler's TeeReader logic.
	// If performance becomes an issue due to concurrent fetches for the *exact* same
	// file at the *exact* same moment, we can implement a more complex locking/coordination
	// mechanism later, perhaps involving caching the response body temporarily in memory
	// for followers if small, or requiring followers to wait and read from cache.

	// --- Sticking with the direct fetch approach without singleflight for now ---
	// This means if 10 requests arrive for the same uncached file, 10 fetches might occur.
	// The cache Put operation *should* be atomic (via temp file rename), so only one will succeed fully.

	return c.doFetch(ctx, upstreamURL, clientHeader)
}

// doFetch performs the actual HTTP GET/HEAD request to the upstream URL.
func (c *Coordinator) doFetch(ctx context.Context, upstreamURL string, clientHeader http.Header) (*Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstreamURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create upstream request: %w", err)
	}

	// Copy relevant client headers (Conditional requests, Range, etc.)
	// Be selective to avoid leaking internal client details or causing issues.
	if ims := clientHeader.Get("If-Modified-Since"); ims != "" {
		req.Header.Set("If-Modified-Since", ims)
	}
	if inm := clientHeader.Get("If-None-Match"); inm != "" {
		req.Header.Set("If-None-Match", inm)
	}
	if rg := clientHeader.Get("Range"); rg != "" {
		// Decide if proxy should handle Range requests or pass them through
		// Passing through might interact poorly with caching full files.
		// For now, don't pass Range, cache expects full objects.
		// req.Header.Set("Range", rg)
		logging.Debug("Ignoring client Range header for upstream request: %s", rg)
	}
	// Set a reasonable User-Agent
	req.Header.Set("User-Agent", "go-apt-cache/1.0 (+https://github.com/yolkispalkis/go-apt-cache-rewrite)")
	req.Header.Set("Accept", "*/*")               // Be generic
	req.Header.Set("Accept-Encoding", "identity") // Avoid upstream compression for now

	logging.Debug("Fetching upstream: %s (If-Modified-Since: %s)", upstreamURL, req.Header.Get("If-Modified-Since"))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Network error, timeout, etc.
		return nil, fmt.Errorf("upstream request failed: %w", err)
	}
	// Note: Caller is responsible for resp.Body.Close() via the Result struct.

	// Handle response codes
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent: // 200 or 206 (if Range was passed)
		// Success
	case http.StatusNotModified: // 304
		resp.Body.Close() // Close body immediately for 304
		return nil, ErrUpstreamNotModified
	case http.StatusNotFound: // 404
		resp.Body.Close() // Close body immediately for 404
		return nil, ErrNotFound
	default:
		// Other errors (4xx client errors, 5xx server errors)
		resp.Body.Close() // Close body immediately
		logging.Error("Upstream returned error status: %d %s for %s", resp.StatusCode, resp.Status, upstreamURL)
		return nil, fmt.Errorf("%w: %d %s", ErrUpstreamError, resp.StatusCode, resp.Status)
	}

	// Parse relevant headers from successful response
	size := int64(-1)
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		parsedSize, err := strconv.ParseInt(cl, 10, 64)
		if err == nil {
			size = parsedSize
		} else {
			logging.Warn("Failed to parse Content-Length header %q: %v", cl, err)
		}
	}

	var modTime time.Time
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		parsedTime, err := http.ParseTime(lm)
		if err == nil {
			modTime = parsedTime
		} else {
			logging.Warn("Failed to parse Last-Modified header %q: %v", lm, err)
		}
	}

	return &Result{
		Body:       resp.Body,
		Header:     resp.Header, // Return the full header map
		Size:       size,
		ModTime:    modTime,
		StatusCode: resp.StatusCode,
	}, nil
}
