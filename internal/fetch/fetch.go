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
	"github.com/yolkispalkis/go-apt-cache/internal/util"
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
	if maxConcurrent <= 0 {
		maxConcurrent = 10
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxConcurrent * 2,
		MaxIdleConnsPerHost:   maxConcurrent,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxConnsPerHost:       maxConcurrent,
		ResponseHeaderTimeout: requestTimeout,
		DisableCompression:    true,
		DisableKeepAlives:     false,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   requestTimeout,
	}

	proxyURL, err := http.ProxyFromEnvironment(&http.Request{URL: &url.URL{Scheme: "http", Host: "example.com"}})
	if err == nil && proxyURL != nil {
		logging.Info("Using system proxy", "proxy_url", proxyURL.String())
	} else if err != nil {
		logging.Warn("Error checking system proxy settings", "error", err)
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
		fetchStartTime := time.Now()

		result, fetchErr := c.doFetch(ctx, upstreamURL, clientHeader)
		fetchDuration := time.Since(fetchStartTime)

		logFields := map[string]interface{}{
			"key":      cacheKey,
			"url":      upstreamURL,
			"duration": util.FormatDuration(fetchDuration),
		}
		if fetchErr != nil {
			logging.Debug("Singleflight fetch completed with error", logFields, "error", fetchErr)
		} else {
			logging.Debug("Singleflight fetch completed successfully", logFields)
		}
		return result, fetchErr
	})

	if err != nil {

		return nil, err
	}

	result, ok := v.(*Result)
	if !ok {

		logging.Error("Internal error: unexpected type returned from singleflight group", "key", cacheKey, "type", fmt.Sprintf("%T", v))
		return nil, fmt.Errorf("internal error: unexpected type returned from singleflight group (%T)", v)
	}

	return result, nil
}

func (c *Coordinator) doFetch(ctx context.Context, upstreamURL string, clientHeader http.Header) (*Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstreamURL, nil)
	if err != nil {
		logging.ErrorE("Failed to create upstream request", err, "url", upstreamURL)
		return nil, fmt.Errorf("%w: failed to create upstream request: %v", ErrRequestConfiguration, err)
	}

	if ims := clientHeader.Get("If-Modified-Since"); ims != "" {
		req.Header.Set("If-Modified-Since", ims)
	}
	if inm := clientHeader.Get("If-None-Match"); inm != "" {
		req.Header.Set("If-None-Match", inm)
	}

	req.Header.Set("User-Agent", "go-apt-proxy/1.0 (+https://github.com/yolkispalkis/go-apt-cache)")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Encoding", "identity")

	logging.Debug("Fetching upstream",
		"url", req.URL.String(),
		"method", req.Method,
		"if_modified_since", req.Header.Get("If-Modified-Since"),
		"if_none_match", req.Header.Get("If-None-Match"),
	)

	resp, err := c.httpClient.Do(req)
	if err != nil {

		if errors.Is(err, context.Canceled) {
			logging.Warn("Upstream request canceled", "url", upstreamURL, "error", err)
			return nil, fmt.Errorf("upstream request canceled: %w", err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			logging.ErrorE("Upstream request deadline exceeded (timeout)", err, "url", upstreamURL, "timeout", c.requestTimeout)
			return nil, fmt.Errorf("upstream request timeout: %w", err)
		}

		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			logging.ErrorE("Upstream request net/http timeout", err, "url", upstreamURL, "timeout", c.requestTimeout)
			return nil, fmt.Errorf("upstream request timeout: %w", err)
		}

		logging.ErrorE("Upstream request failed", err, "url", upstreamURL)
		return nil, fmt.Errorf("upstream request failed: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		logging.Debug("Upstream fetch successful", "url", upstreamURL, "status_code", resp.StatusCode)

	case http.StatusNotModified:
		resp.Body.Close()
		logging.Debug("Upstream returned 304 Not Modified", "url", upstreamURL)
		return nil, ErrUpstreamNotModified
	case http.StatusNotFound:
		resp.Body.Close()
		logging.Warn("Upstream returned 404 Not Found", "url", upstreamURL)
		return nil, ErrNotFound
	default:

		statusCode := resp.StatusCode
		statusText := resp.Status
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		logging.Error("Upstream returned error status",
			"status_code", statusCode,
			"status_text", statusText,
			"url", upstreamURL,
			"response_snippet", string(bodyBytes),
		)

		return nil, fmt.Errorf("%w: %s", ErrUpstreamError, statusText)
	}

	size := int64(-1)
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		parsedSize, parseErr := strconv.ParseInt(cl, 10, 64)
		if parseErr == nil && parsedSize >= 0 {
			size = parsedSize
		} else {
			logging.Warn("Failed to parse Content-Length header or invalid value", "error", parseErr, "content_length_header", cl, "url", upstreamURL)
		}
	}

	var modTime time.Time
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		parsedTime, parseErr := http.ParseTime(lm)
		if parseErr == nil {
			modTime = parsedTime
		} else {
			logging.Warn("Failed to parse Last-Modified header", "error", parseErr, "last_modified_header", lm, "url", upstreamURL)
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
