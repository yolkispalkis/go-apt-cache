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
	"strings"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

var (
	ErrNotFound             = errors.New("upstream resource not found (404)")
	ErrUpstreamNotModified  = errors.New("upstream resource not modified (304)")
	ErrUpstreamError        = errors.New("upstream server error")
	ErrRequestConfiguration = errors.New("invalid request configuration")
	ErrCacheWriteFailed     = errors.New("failed to write fetched item to cache")
	ErrInternalFetch        = errors.New("internal fetcher error")
)

type fetchResultInternal struct {
	Body       io.ReadCloser
	Header     http.Header
	Size       int64
	ModTime    time.Time
	StatusCode int
}

type Coordinator struct {
	httpClient   *http.Client
	fetchGroup   singleflight.Group
	cacheManager cache.CacheManager
}

func NewCoordinator(requestTimeout time.Duration, maxConcurrent int, cm cache.CacheManager) *Coordinator {
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
		httpClient:   client,
		cacheManager: cm,
	}
}

func (c *Coordinator) FetchAndCache(ctx context.Context, cacheKey, upstreamURL, relativePath string, clientHeader http.Header) error {
	_, err, _ := c.fetchGroup.Do(cacheKey, func() (interface{}, error) {
		fetchStartTime := time.Now()
		logging.Debug("Singleflight: Initiating fetch and cache", "key", cacheKey, "url", upstreamURL)

		fetchResult, fetchErr := c.doFetch(ctx, upstreamURL, clientHeader)
		if fetchErr != nil {
			if errors.Is(fetchErr, ErrUpstreamNotModified) {
				logging.Error("Singleflight: Unexpected 304 from upstream during initial fetch", "key", cacheKey, "url", upstreamURL)
				return nil, fmt.Errorf("%w: unexpected 304 during initial fetch for %s", ErrUpstreamError, cacheKey)
			}
			logging.ErrorE("Singleflight: Fetch failed", fetchErr, "key", cacheKey, "url", upstreamURL)
			return nil, fmt.Errorf("fetch failed for %s: %w", cacheKey, fetchErr)
		}
		defer fetchResult.Body.Close()

		cachePutMeta := cache.CacheMetadata{
			Version:   cache.MetadataVersion,
			FetchTime: time.Now().UTC(),
			ModTime:   fetchResult.ModTime,
			Size:      fetchResult.Size,
			Headers:   make(http.Header),
			Key:       cacheKey,
		}
		finalContentType := ""
		upstreamContentType := fetchResult.Header.Get("Content-Type")
		if upstreamContentType != "" && !strings.HasPrefix(strings.ToLower(upstreamContentType), "application/octet-stream") {
			finalContentType = upstreamContentType
		} else {
			finalContentType = util.GetContentType(relativePath)
		}
		if finalContentType == "" {
			finalContentType = "application/octet-stream"
		}
		cachePutMeta.Headers.Set("Content-Type", finalContentType)
		util.SelectCacheControlHeaders(cachePutMeta.Headers, fetchResult.Header)

		putErr := c.cacheManager.Put(ctx, cacheKey, fetchResult.Body, cachePutMeta)
		if putErr != nil {
			logging.ErrorE("Singleflight: Failed to write item to cache", putErr, "key", cacheKey)
			_ = c.cacheManager.Delete(context.Background(), cacheKey)
			return nil, fmt.Errorf("%w: %w", ErrCacheWriteFailed, putErr)
		}

		fetchDuration := time.Since(fetchStartTime)
		logging.Info("Singleflight: Fetch and cache successful",
			"key", cacheKey,
			"url", upstreamURL,
			"size", cachePutMeta.Size,
			"duration", util.FormatDuration(fetchDuration),
		)
		return nil, nil
	})

	return err
}

func (c *Coordinator) Fetch(ctx context.Context, fetchKey, upstreamURL string, clientHeader http.Header) (*fetchResultInternal, error) {
	v, err, _ := c.fetchGroup.Do(fetchKey, func() (interface{}, error) {
		fetchStartTime := time.Now()
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
		return nil, fmt.Errorf("%w: unexpected type from singleflight (%T)", ErrInternalFetch, v)
	}
	return result, nil
}

func (c *Coordinator) FetchForRevalidation(ctx context.Context, revalKey, upstreamURL string, conditionalHeaders http.Header) (*fetchResultInternal, error) {
	logging.Debug("Revalidation: Sending conditional request via Fetch", "key", revalKey, "url", upstreamURL, "headers", conditionalHeaders)
	fetchResult, fetchErr := c.Fetch(ctx, revalKey, upstreamURL, conditionalHeaders)

	if fetchErr != nil {
		if errors.Is(fetchErr, ErrUpstreamNotModified) {
			logging.Debug("Revalidation: Success (304 Not Modified)", "key", revalKey, "url", upstreamURL)
			return nil, ErrUpstreamNotModified
		}
		logging.Warn("Revalidation: Fetch failed", "error", fetchErr, "key", revalKey, "url", upstreamURL)
		return nil, fmt.Errorf("revalidation fetch failed for %s: %w", revalKey, fetchErr)
	}

	logging.Debug("Revalidation: Resource changed (2xx received)", "key", revalKey, "url", upstreamURL, "status", fetchResult.StatusCode)
	return fetchResult, nil
}

func (c *Coordinator) doFetch(ctx context.Context, upstreamURL string, clientHeader http.Header) (*fetchResultInternal, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstreamURL, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create upstream request: %w", ErrRequestConfiguration, err)
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

	resp, err := c.httpClient.Do(req)
	if err != nil {

		if errors.Is(err, context.Canceled) {
			return nil, fmt.Errorf("upstream request canceled: %w", err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("upstream request deadline exceeded: %w", err)
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return nil, fmt.Errorf("upstream request timeout: %w", err)
		}

		return nil, fmt.Errorf("upstream request failed: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:

	case http.StatusNotModified:
		resp.Body.Close()
		return nil, ErrUpstreamNotModified
	case http.StatusNotFound:
		resp.Body.Close()
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
		}
	}

	var modTime time.Time
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		parsedTime, parseErr := http.ParseTime(lm)
		if parseErr == nil {
			modTime = parsedTime
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
