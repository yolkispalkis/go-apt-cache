package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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
		MaxConnsPerHost:       maxConcurrent,
		DisableKeepAlives:     false,
		ResponseHeaderTimeout: requestTimeout,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   requestTimeout,
	}

	proxyURL, err := http.ProxyFromEnvironment(&http.Request{URL: &url.URL{Scheme: "http", Host: "example.com"}})
	if err == nil && proxyURL != nil {
		logging.Info("Using system proxy: %s", proxyURL)
	} else if err != nil {
		logging.Warn("Error checking system proxy settings: %v", err)
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

		return nil, fmt.Errorf("internal error: unexpected type returned from singleflight group (%T)", v)
	}

	return result, nil
}

func (c *Coordinator) doFetch(ctx context.Context, upstreamURL string, clientHeader http.Header) (*Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstreamURL, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create upstream request: %v", ErrRequestConfiguration, err)
	}

	if ims := clientHeader.Get("If-Modified-Since"); ims != "" {
		req.Header.Set("If-Modified-Since", ims)
	}
	if inm := clientHeader.Get("If-None-Match"); inm != "" {
		req.Header.Set("If-None-Match", inm)
	}

	req.Header.Set("User-Agent", "go-apt-proxy/1.0")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Encoding", "identity")

	logging.Debug("Fetching upstream: %s (If-Modified-Since: %s)", upstreamURL, req.Header.Get("If-Modified-Since"))

	resp, err := c.httpClient.Do(req)
	if err != nil {

		if errors.Is(err, context.Canceled) {
			logging.Warn("Upstream request canceled for %s: %v", upstreamURL, err)
			return nil, fmt.Errorf("upstream request canceled: %w", err)
		}

		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			logging.Error("Upstream request timeout for %s: %v", upstreamURL, err)
			return nil, fmt.Errorf("upstream request timeout: %w", err)
		}
		logging.Error("Upstream request failed for %s: %v", upstreamURL, err)
		return nil, fmt.Errorf("upstream request failed: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:

	case http.StatusNotModified:
		resp.Body.Close()
		logging.Debug("Upstream returned 304 Not Modified for %s", upstreamURL)
		return nil, ErrUpstreamNotModified
	case http.StatusNotFound:
		resp.Body.Close()
		logging.Warn("Upstream returned 404 Not Found for %s", upstreamURL)
		return nil, ErrNotFound

	default:

		statusCode := resp.StatusCode
		statusText := resp.Status
		resp.Body.Close()
		logging.Error("Upstream returned error status: %d %s for %s", statusCode, statusText, upstreamURL)

		return nil, fmt.Errorf("%w: %d %s", ErrUpstreamError, statusCode, statusText)
	}

	size := int64(-1)
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		parsedSize, err := strconv.ParseInt(cl, 10, 64)
		if err == nil {
			size = parsedSize
		} else {
			logging.Warn("Failed to parse Content-Length header %q for %s: %v", cl, upstreamURL, err)
		}
	}

	var modTime time.Time
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		parsedTime, err := http.ParseTime(lm)
		if err == nil {
			modTime = parsedTime
		} else {
			logging.Warn("Failed to parse Last-Modified header %q for %s: %v", lm, upstreamURL, err)
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
