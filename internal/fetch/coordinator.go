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

	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
	"golang.org/x/sync/singleflight"
)

var (
	ErrUpstreamNotModified = errors.New("upstream: not modified (304)")
	ErrUpstreamNotFound    = errors.New("upstream: not found (404)")
	ErrUpstreamClient      = errors.New("upstream: client error (4xx)")
	ErrUpstreamServer      = errors.New("upstream: server error (5xx)")
	ErrRequestSetup        = errors.New("fetch: request setup failed")
	ErrNetwork             = errors.New("fetch: network error")
	ErrInternal            = errors.New("fetch: internal error")
)

var hopByHopHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
	"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

type Result struct {
	Status int
	Header http.Header
	Body   io.ReadCloser
	Size   int64
}

type Options struct {
	IfModSince  time.Time
	IfNoneMatch string
	UseHEAD     bool
}

type SharedFetch struct {
	Result *Result
	Err    error
}

type Coordinator struct {
	client    *http.Client
	sfGroup   singleflight.Group
	userAgent string
	log       *logging.Logger
}

func NewCoordinator(cfg config.ServerConfig, logger *logging.Logger) *Coordinator {
	log := logger.WithComponent("fetchCoordinator")

	maxConns := cfg.MaxConcurrent
	if maxConns <= 0 {
		maxConns = 20
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,
		MaxIdleConns:        maxConns * 2,
		MaxIdleConnsPerHost: maxConns,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		DisableCompression:  true,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.ReqTimeout,
	}

	logProxyInfo(log)

	return &Coordinator{
		client:    client,
		userAgent: cfg.UserAgent,
		log:       log,
	}
}

func (c *Coordinator) Fetch(ctx context.Context, key, upstreamURL string, opts *Options) (any, error, bool) {
	if err := ctx.Err(); err != nil {
		return nil, err, false
	}

	resInterface, err, shared := c.sfGroup.Do(key, func() (any, error) {
		c.log.Debug().Str("key", key).Msg("Executing actual fetch")
		result, fetchErr := c.doFetch(ctx, upstreamURL, opts)
		return &SharedFetch{Result: result, Err: fetchErr}, fetchErr
	})

	if shared {
		c.log.Debug().Str("key", key).Msg("Shared fetch result")
	}
	return resInterface, err, shared
}

func (c *Coordinator) doFetch(ctx context.Context, upstreamURL string, opts *Options) (*Result, error) {
	method := http.MethodGet
	if opts != nil && opts.UseHEAD {
		method = http.MethodHead
	}

	req, err := http.NewRequestWithContext(ctx, method, upstreamURL, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRequestSetup, err)
	}

	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "*/*")

	if opts != nil {
		if !opts.IfModSince.IsZero() {
			req.Header.Set("If-Modified-Since", opts.IfModSince.UTC().Format(http.TimeFormat))
		}
		if opts.IfNoneMatch != "" {
			req.Header.Set("If-None-Match", opts.IfNoneMatch)
		}
	}

	resp, err := c.client.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %w", ErrNetwork, err)
	}

	hdr := util.CopyHeader(resp.Header)
	for _, h := range hopByHopHeaders {
		hdr.Del(h)
	}
	result := &Result{Status: resp.StatusCode, Header: hdr}

	switch {
	case resp.StatusCode == http.StatusNotModified:
		resp.Body.Close()
		return result, ErrUpstreamNotModified

	case resp.StatusCode == http.StatusNotFound:
		resp.Body.Close()
		return result, ErrUpstreamNotFound

	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		// тело возвращаем только для GET
		if method == http.MethodGet {
			result.Body = resp.Body
			if cl := resp.Header.Get("Content-Length"); cl != "" {
				if sz, err := strconv.ParseInt(cl, 10, 64); err == nil {
					result.Size = sz
				}
			}
		} else {
			resp.Body.Close()
		}
		return result, nil

	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		resp.Body.Close()
		return result, fmt.Errorf("%w (status %d)", ErrUpstreamClient, resp.StatusCode)

	default:
		resp.Body.Close()
		return result, fmt.Errorf("%w (status %d)", ErrUpstreamServer, resp.StatusCode)
	}
}

func NewFetchOptions(r *http.Request, meta *cache.ItemMeta) *Options {
	if meta != nil {
		o := &Options{IfNoneMatch: meta.Headers.Get("ETag")}
		if t, err := http.ParseTime(meta.Headers.Get("Last-Modified")); err == nil {
			o.IfModSince = t
		}
		return o
	}
	o := &Options{IfNoneMatch: r.Header.Get("If-None-Match")}
	if t, err := http.ParseTime(r.Header.Get("If-Modified-Since")); err == nil {
		o.IfModSince = t
	}
	return o
}

func logProxyInfo(log *logging.Logger) {
	for _, s := range []string{"http", "https"} {
		reqURL, _ := url.Parse(fmt.Sprintf("%s://example.com", s))
		proxyURL, err := http.ProxyFromEnvironment(&http.Request{URL: reqURL})
		if err != nil {
			log.Error().Err(err).Str("scheme", s).Msg("Proxy detection error")
			continue
		}
		if proxyURL != nil {
			noAuth := *proxyURL
			noAuth.User = nil
			log.Info().Str("scheme", s).Str("proxy_url", noAuth.String()).Msg("System proxy configured")
		} else {
			log.Info().Str("scheme", s).Msg("No system proxy configured")
		}
	}
}
