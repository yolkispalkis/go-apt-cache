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

type fetchResultInternal struct {
	Body       io.ReadCloser
	Header     http.Header
	Size       int64
	ModTime    time.Time
	StatusCode int
}

type Coordinator struct {
	httpClient *http.Client
	fetchGroup singleflight.Group
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
		logProxyURL := *proxyURL
		logProxyURL.User = nil
		logging.Info("Using system proxy", "proxy_url", logProxyURL.String())
	} else if err != nil {
		logging.Warn("Error checking system proxy settings", "error", err)
	} else {
		logging.Info("No system proxy configured or detected.")
	}

	return &Coordinator{
		httpClient: client,
	}
}

func (c *Coordinator) Fetch(ctx context.Context, fetchKey, upstreamURL string, clientHeader http.Header) (*fetchResultInternal, error) {

	v, err, _ := c.fetchGroup.Do(fetchKey, func() (interface{}, error) {
		fetchStartTime := time.Now()
		logging.Debug("Singleflight: Initiating upstream fetch", "key", fetchKey, "url", upstreamURL)

		result, fetchErr := c.doFetch(ctx, upstreamURL, clientHeader)

		fetchDuration := time.Since(fetchStartTime)
		if fetchErr != nil {
			logging.Debug("Singleflight: Fetch completed with error",
				"key", fetchKey, "url", upstreamURL, "duration", util.FormatDuration(fetchDuration), "error", fetchErr)
		} else {
			logging.Debug("Singleflight: Fetch completed successfully",
				"key", fetchKey, "url", upstreamURL, "duration", util.FormatDuration(fetchDuration), "status", result.StatusCode)
		}

		return result, fetchErr
	})

	if err != nil {
		return nil, err
	}

	result, ok := v.(*fetchResultInternal)
	if !ok {

		if rc, okCloser := v.(io.ReadCloser); okCloser {
			_ = rc.Close()
		}
		logging.Error("Internal error: unexpected type returned from fetch singleflight group", "key", fetchKey, "type", fmt.Sprintf("%T", v))
		return nil, fmt.Errorf("internal error: unexpected type returned from fetch singleflight group (%T)", v)
	}

	return result, nil
}

func (c *Coordinator) FetchForRevalidation(ctx context.Context, revalKey, upstreamURL string, conditionalHeaders http.Header) (*fetchResultInternal, error) {

	logging.Debug("Revalidation: Sending conditional request via Fetch", "key", revalKey, "url", upstreamURL, "headers", conditionalHeaders)
	fetchResult, fetchErr := c.Fetch(ctx, revalKey, upstreamURL, conditionalHeaders)

	if fetchErr != nil {

		if errors.Is(fetchErr, ErrUpstreamNotModified) {
			logging.Debug("Revalidation: Success (304 Not Modified)", "key", revalKey, "url", upstreamURL)
			return nil, nil
		}

		logging.Warn("Revalidation: Fetch failed", "error", fetchErr, "key", revalKey, "url", upstreamURL)
		return nil, fetchErr
	}

	logging.Debug("Revalidation: Resource changed (2xx received)", "key", revalKey, "url", upstreamURL, "status", fetchResult.StatusCode)
	return fetchResult, nil
}

func (c *Coordinator) doFetch(ctx context.Context, upstreamURL string, clientHeader http.Header) (*fetchResultInternal, error) {
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
			logging.ErrorE("Upstream request deadline exceeded (timeout)", err, "url", upstreamURL)
			return nil, fmt.Errorf("upstream request timeout: %w", err)
		}
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			logging.ErrorE("Upstream request net/http timeout", err, "url", upstreamURL)
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

	return &fetchResultInternal{
		Body:       resp.Body,
		Header:     resp.Header,
		Size:       size,
		ModTime:    modTime,
		StatusCode: resp.StatusCode,
	}, nil
}
