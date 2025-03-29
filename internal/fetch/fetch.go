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

type Result struct {
	Body       io.ReadCloser
	Header     http.Header
	Size       int64
	ModTime    time.Time
	StatusCode int
}

type Coordinator struct {
	httpClient     *http.Client
	fetchGroup     singleflight.Group
	requestTimeout time.Duration
}

func NewCoordinator(requestTimeout time.Duration, maxConcurrent int) *Coordinator {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          maxConcurrent * 2,
		MaxIdleConnsPerHost:   maxConcurrent,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   requestTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			logging.Warn("Upstream redirect detected for %s. Redirects are currently not followed.", req.URL)
			return http.ErrUseLastResponse
		},
	}

	proxyURL, err := http.ProxyFromEnvironment(&http.Request{URL: &url.URL{Scheme: "http", Host: "example.com"}})
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

func (c *Coordinator) Fetch(ctx context.Context, cacheKey, upstreamURL string, clientHeader http.Header) (*Result, error) {
	v, err, _ := c.fetchGroup.Do(cacheKey, func() (interface{}, error) {
		return c.doFetch(ctx, upstreamURL, clientHeader)
	})

	if err != nil {
		return nil, err
	}

	result, ok := v.(*Result)
	if !ok {
		return nil, fmt.Errorf("internal error: unexpected type returned from singleflight group")
	}

	return result, nil
}

func (c *Coordinator) doFetch(ctx context.Context, upstreamURL string, clientHeader http.Header) (*Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstreamURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create upstream request: %w", err)
	}

	if ims := clientHeader.Get("If-Modified-Since"); ims != "" {
		req.Header.Set("If-Modified-Since", ims)
	}
	if inm := clientHeader.Get("If-None-Match"); inm != "" {
		req.Header.Set("If-None-Match", inm)
	}
	if rg := clientHeader.Get("Range"); rg != "" {
		logging.Debug("Ignoring client Range header for upstream request: %s", rg)
	}
	req.Header.Set("User-Agent", "go-apt-cache/1.0 (+https://github.com/yolkispalkis/go-apt-cache-rewrite)")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Encoding", "identity")

	logging.Debug("Fetching upstream: %s (If-Modified-Since: %s)", upstreamURL, req.Header.Get("If-Modified-Since"))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("upstream request failed: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		// Success
	case http.StatusNotModified:
		resp.Body.Close()
		return nil, ErrUpstreamNotModified
	case http.StatusNotFound:
		resp.Body.Close()
		return nil, ErrNotFound
	default:
		resp.Body.Close()
		logging.Error("Upstream returned error status: %d %s for %s", resp.StatusCode, resp.Status, upstreamURL)
		return nil, fmt.Errorf("%w: %d %s", ErrUpstreamError, resp.StatusCode, resp.Status)
	}

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
		Header:     resp.Header,
		Size:       size,
		ModTime:    modTime,
		StatusCode: resp.StatusCode,
	}, nil
}
