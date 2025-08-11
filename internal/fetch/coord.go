package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/log"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

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
	Range       string
}

type Coordinator struct {
	client *http.Client
	sf     singleflight.Group
	ua     string
	log    *log.Logger
	sem    chan struct{}
}

func New(cfg config.ServerConfig, lg *log.Logger) *Coordinator {
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 60 * time.Second}).DialContext,
		MaxIdleConns:        cfg.MaxConcurrent * 2,
		MaxIdleConnsPerHost: cfg.MaxConcurrent,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		DisableCompression:  true,
	}
	if cfg.MaxConcurrent < 1 {
		cfg.MaxConcurrent = 1
	}
	return &Coordinator{
		client: &http.Client{Transport: tr, Timeout: cfg.ReqTimeout},
		ua:     cfg.UserAgent,
		log:    lg.WithComponent("fetch"),
		sem:    make(chan struct{}, cfg.MaxConcurrent),
	}
}

func (c *Coordinator) Fetch(ctx context.Context, key, url string, o *Options) (*Result, error, bool) {
	if err := ctx.Err(); err != nil {
		return nil, err, false
	}
	v, err, shared := c.sf.Do(key, func() (any, error) {
		select {
		case c.sem <- struct{}{}:
			defer func() { <-c.sem }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return c.do(ctx, url, o)
	})
	if err != nil {
		return nil, err, shared
	}
	return v.(*Result), nil, shared
}

func (c *Coordinator) do(ctx context.Context, url string, o *Options) (*Result, error) {
	m := http.MethodGet
	if o != nil && o.UseHEAD {
		m = http.MethodHead
	}
	req, err := http.NewRequestWithContext(ctx, m, url, nil)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	req.Header.Set("User-Agent", c.ua)
	req.Header.Set("Accept", "*/*")
	req.Header.Add("Via", "1.1 go-apt-cache")
	if o != nil {
		if !o.IfModSince.IsZero() {
			req.Header.Set("If-Modified-Since", o.IfModSince.UTC().Format(http.TimeFormat))
		}
		if o.IfNoneMatch != "" {
			req.Header.Set("If-None-Match", o.IfNoneMatch)
		}
		if o.Range != "" {
			req.Header.Set("Range", o.Range)
		}
	}

	resp, err := c.client.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("network: %w", err)
	}

	h := util.CopyHeader(resp.Header)
	for _, k := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade"} {
		h.Del(k)
	}
	res := &Result{Status: resp.StatusCode, Header: h}
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		if n, err := strconv.ParseInt(cl, 10, 64); err == nil {
			res.Size = n
		}
	}

	switch {
	case resp.StatusCode == http.StatusNotModified, resp.StatusCode == http.StatusNotFound:
		resp.Body.Close()
		return res, nil
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if m == http.MethodGet {
			res.Body = resp.Body
		} else {
			resp.Body.Close()
		}
		return res, nil
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// forward 4xx to caller (except 404 already handled)
		if m == http.MethodGet {
			res.Body = resp.Body
		} else {
			resp.Body.Close()
		}
		return res, nil
	default:
		resp.Body.Close()
		return res, fmt.Errorf("upstream 5xx: %d", resp.StatusCode)
	}
}

func NewOptions(r *http.Request, etag, lastMod string) *Options {
	o := &Options{IfNoneMatch: r.Header.Get("If-None-Match")}
	if etag != "" {
		o.IfNoneMatch = etag
	}
	if t, err := http.ParseTime(r.Header.Get("If-Modified-Since")); err == nil {
		o.IfModSince = t
	}
	if lastMod != "" {
		if t, err := http.ParseTime(lastMod); err == nil {
			o.IfModSince = t
		}
	}
	o.Range = r.Header.Get("Range")
	return o
}
