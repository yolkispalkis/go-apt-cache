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

	"github.com/rs/zerolog"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
	"golang.org/x/sync/singleflight"
)

var (
	ErrUpstreamNotModified = errors.New("upstream: not modified (304)")
	ErrUpstreamNotFound    = errors.New("upstream: not found (404)")
	ErrUpstreamClientErr   = errors.New("upstream: other client error (4xx)")
	ErrUpstreamServerErr   = errors.New("upstream: server error (5xx)")
	ErrReqSetup            = errors.New("fetch: request setup failed")
	ErrNetwork             = errors.New("fetch: network error")
	ErrInternal            = errors.New("fetch: internal error")
)

type Result struct {
	Status  int
	Header  http.Header
	Body    io.ReadCloser
	Size    int64
	ModTime time.Time
}

type Options struct {
	IfModSince  time.Time
	IfNoneMatch string
}

type Coordinator struct {
	client    *http.Client
	sfGroup   singleflight.Group
	userAgent string
	log       zerolog.Logger
}

func NewCoordinator(cfg config.ServerConfig, logger zerolog.Logger) *Coordinator {

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,
		MaxIdleConns:          cfg.MaxConcurrent * 2,
		MaxIdleConnsPerHost:   cfg.MaxConcurrent,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
		ResponseHeaderTimeout: cfg.ReqTimeout.StdDuration(),
		DisableCompression:    true,
	}

	client := &http.Client{
		Transport: transport,

		Timeout: cfg.ReqTimeout.StdDuration() + (5 * time.Second),
	}

	dummyReqURL, _ := url.Parse("http://example.com")
	proxyURL, _ := http.ProxyFromEnvironment(&http.Request{URL: dummyReqURL})
	if proxyURL != nil {
		safeProxyURL := *proxyURL
		safeProxyURL.User = nil
		logger.Info().Str("proxy_url", safeProxyURL.String()).Msg("Using system proxy for upstream requests")
	}

	return &Coordinator{
		client:    client,
		userAgent: cfg.UserAgent,
		log:       logger.With().Str("component", "fetchCoordinator").Logger(),
	}
}

func (c *Coordinator) Fetch(ctx context.Context, key, upstreamURL string, opts *Options) (*Result, error) {
	c.log.Debug().Str("key", key).Str("url", upstreamURL).Interface("opts", opts).Msg("Attempting to fetch resource")

	resInterface, err, shared := c.sfGroup.Do(key, func() (any, error) {
		c.log.Debug().Str("key", key).Msg("Executing actual fetch (singleflight leader)")
		return c.doFetch(ctx, upstreamURL, opts)
	})

	if shared {
		c.log.Debug().Str("key", key).Msg("Shared fetch result with other goroutines")
	}
	if err != nil {
		c.log.Warn().Err(err).Str("key", key).Str("url", upstreamURL).Bool("shared", shared).Msg("Fetch returned error")
		return nil, err
	}

	result, ok := resInterface.(*Result)
	if !ok {

		c.log.Error().Str("key", key).Str("type", fmt.Sprintf("%T", resInterface)).Msg("Internal error: unexpected type from singleflight")
		return nil, fmt.Errorf("%w: unexpected type %T from singleflight", ErrInternal, resInterface)
	}

	c.log.Debug().Str("key", key).Int("status", result.Status).Msg("Fetch successful")
	return result, nil
}

func (c *Coordinator) doFetch(ctx context.Context, upstreamURL string, opts *Options) (*Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, upstreamURL, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: creating request: %w", ErrReqSetup, err)
	}

	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Accept", "*/*")

	if opts != nil {
		if !opts.IfModSince.IsZero() {
			req.Header.Set("If-Modified-Since", opts.IfModSince.UTC().Format(http.TimeFormat))
		}
		if opts.IfNoneMatch != "" {
			req.Header.Set("If-None-Match", opts.IfNoneMatch)
		}
	}
	c.log.Debug().Str("url", upstreamURL).Interface("headers", req.Header).Msg("Sending upstream request")

	resp, err := c.client.Do(req)
	if err != nil {

		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, context.DeadlineExceeded
		}

		c.log.Error().Err(err).Str("url", upstreamURL).Msg("Upstream HTTP request failed")
		return nil, fmt.Errorf("%w: %w", ErrNetwork, err)
	}
	c.log.Debug().Str("url", upstreamURL).Int("status", resp.StatusCode).Msg("Received upstream response")

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
		return nil, fmt.Errorf("%w (status %d)", ErrUpstreamClientErr, resp.StatusCode)
	case resp.StatusCode >= 500:
		resp.Body.Close()
		return nil, fmt.Errorf("%w (status %d)", ErrUpstreamServerErr, resp.StatusCode)
	default:
		resp.Body.Close()
		return nil, fmt.Errorf("%w: unexpected status %d", ErrInternal, resp.StatusCode)
	}

	fetchRes := &Result{
		Status: resp.StatusCode,
		Header: util.CopyHeader(resp.Header),
		Body:   resp.Body,
		Size:   -1,
	}

	if cl := resp.Header.Get("Content-Length"); cl != "" {
		if size, err := strconv.ParseInt(cl, 10, 64); err == nil && size >= 0 {
			fetchRes.Size = size
		} else {
			c.log.Warn().Str("url", upstreamURL).Str("content_length", cl).Err(err).Msg("Invalid Content-Length header from upstream")
		}
	}

	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		if modTime, err := http.ParseTime(lm); err == nil {
			fetchRes.ModTime = modTime
		} else {
			c.log.Warn().Str("url", upstreamURL).Str("last_modified", lm).Err(err).Msg("Invalid Last-Modified header from upstream")
		}
	}
	return fetchRes, nil
}
