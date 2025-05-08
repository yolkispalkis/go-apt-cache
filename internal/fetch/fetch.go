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

	"github.com/rs/zerolog"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

var (
	ErrUpstreamNotModified = errors.New("upstream: not modified (304)")
	ErrUpstreamNotFound    = errors.New("upstream: not found (404)")
	ErrUpstreamOtherClient = errors.New("upstream: other client error (4xx)")
	ErrUpstreamServer      = errors.New("upstream: server error (5xx)")
	ErrRequestSetup        = errors.New("fetch: request setup failed")
	ErrNetwork             = errors.New("fetch: network error")
	ErrInternal            = errors.New("fetch: internal error")
)

type FetchResult struct {
	StatusCode int
	Header     http.Header
	Body       io.ReadCloser
	Size       int64
	ModTime    time.Time
}

type Coordinator struct {
	httpClient *http.Client
	group      singleflight.Group
	userAgent  string
	logger     zerolog.Logger
}

func NewCoordinator(cfg config.ServerConfig, logger zerolog.Logger) *Coordinator {

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,
		MaxIdleConns:          cfg.MaxConcurrentFetches * 2,
		MaxIdleConnsPerHost:   cfg.MaxConcurrentFetches,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
		ResponseHeaderTimeout: cfg.RequestTimeout.Duration(),

		DisableCompression: true,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.RequestTimeout.Duration() + (5 * time.Second),
	}

	proxyURL, _ := http.ProxyFromEnvironment(&http.Request{URL: &url.URL{Scheme: "http", Host: "example.com"}})
	if proxyURL != nil {

		safeProxyURL := *proxyURL
		safeProxyURL.User = nil
		logger.Info().Str("proxy_url", safeProxyURL.String()).Msg("Using system proxy for upstream requests")
	}

	return &Coordinator{
		httpClient: client,
		userAgent:  cfg.UserAgent,
		logger:     logger.With().Str("component", "fetchCoordinator").Logger(),
	}
}

type FetchOptions struct {
	IfModifiedSince time.Time
	IfNoneMatch     string
}

func (c *Coordinator) Fetch(ctx context.Context, key string, upstreamURL string, opts *FetchOptions) (*FetchResult, error) {
	c.logger.Debug().Str("key", key).Str("url", upstreamURL).Interface("opts", opts).Msg("Attempting to fetch resource")

	resInterface, err, shared := c.group.Do(key, func() (interface{}, error) {
		c.logger.Debug().Str("key", key).Msg("Executing actual fetch (singleflight leader)")
		return c.doFetch(ctx, upstreamURL, opts)
	})

	if shared {
		c.logger.Debug().Str("key", key).Msg("Shared fetch result with other goroutines")
	}

	if err != nil {

		c.logger.Warn().Err(err).Str("key", key).Str("url", upstreamURL).Bool("shared", shared).Msg("Fetch returned error")
		return nil, err
	}

	result, ok := resInterface.(*FetchResult)
	if !ok {

		c.logger.Error().Str("key", key).Str("type", fmt.Sprintf("%T", resInterface)).Msg("Internal error: unexpected type from singleflight")
		return nil, fmt.Errorf("%w: unexpected type from singleflight (%T)", ErrInternal, resInterface)
	}

	c.logger.Debug().Str("key", key).Int("status", result.StatusCode).Msg("Fetch successful")
	return result, nil
}

func (c *Coordinator) doFetch(ctx context.Context, upstreamURL string, opts *FetchOptions) (*FetchResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstreamURL, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: creating request: %w", ErrRequestSetup, err)
	}

	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Accept", "*/*")

	if opts != nil {
		if !opts.IfModifiedSince.IsZero() {
			req.Header.Set("If-Modified-Since", opts.IfModifiedSince.UTC().Format(http.TimeFormat))
		}
		if opts.IfNoneMatch != "" {
			req.Header.Set("If-None-Match", opts.IfNoneMatch)
		}
	}

	c.logger.Debug().Str("url", upstreamURL).Interface("headers", req.Header).Msg("Sending upstream request")

	resp, err := c.httpClient.Do(req)
	if err != nil {

		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, context.DeadlineExceeded
		}

		c.logger.Error().Err(err).Str("url", upstreamURL).Msg("Upstream HTTP request failed")
		return nil, fmt.Errorf("%w: %w", ErrNetwork, err)
	}

	c.logger.Debug().Str("url", upstreamURL).Int("status", resp.StatusCode).Msg("Received upstream response")

	switch {
	case resp.StatusCode == http.StatusNotModified:
		resp.Body.Close()
		return nil, ErrUpstreamNotModified
	case resp.StatusCode == http.StatusNotFound:
		resp.Body.Close()
		return nil, ErrUpstreamNotFound
	case resp.StatusCode >= 200 && resp.StatusCode < 300:

	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		resp.Body.Close()
		return nil, fmt.Errorf("%w: status %d", ErrUpstreamOtherClient, resp.StatusCode)
	case resp.StatusCode >= 500:
		resp.Body.Close()
		return nil, fmt.Errorf("%w: status %d", ErrUpstreamServer, resp.StatusCode)
	default:
		resp.Body.Close()
		return nil, fmt.Errorf("%w: unexpected status %d", ErrInternal, resp.StatusCode)
	}

	fetchRes := &FetchResult{
		StatusCode: resp.StatusCode,
		Header:     util.CopyHeader(resp.Header),
		Body:       resp.Body,
		Size:       -1,
	}

	if cl := resp.Header.Get("Content-Length"); cl != "" {
		size, err := strconv.ParseInt(cl, 10, 64)
		if err == nil && size >= 0 {
			fetchRes.Size = size
		} else {
			c.logger.Warn().Str("url", upstreamURL).Str("content_length", cl).Err(err).Msg("Invalid Content-Length header from upstream")
		}
	}

	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		modTime, err := http.ParseTime(lm)
		if err == nil {
			fetchRes.ModTime = modTime
		} else {
			c.logger.Warn().Str("url", upstreamURL).Str("last_modified", lm).Err(err).Msg("Invalid Last-Modified header from upstream")
		}
	}

	return fetchRes, nil
}
