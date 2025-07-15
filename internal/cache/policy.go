package cache

import (
	"net/http"
	"strconv"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

func CalculateFreshness(headers http.Header, responseTime time.Time, relPath string, overrides []config.CacheOverride) time.Time {
	if overrideTTL, ok := findOverrideTTL(relPath, overrides); ok {
		return responseTime.Add(overrideTTL)
	}
	cc := util.ParseCacheControl(headers.Get("Cache-Control"))
	if _, ok := cc["no-store"]; ok {
		return time.Time{}
	}
	if _, ok := cc["no-cache"]; ok {
		return responseTime
	}
	var lifetime time.Duration
	if sMaxAge, ok := cc["s-maxage"]; ok {
		if sec, err := strconv.ParseInt(sMaxAge, 10, 64); err == nil {
			lifetime = time.Duration(sec) * time.Second
		}
	} else if maxAge, ok := cc["max-age"]; ok {
		if sec, err := strconv.ParseInt(maxAge, 10, 64); err == nil {
			lifetime = time.Duration(sec) * time.Second
		}
	} else if expiresStr := headers.Get("Expires"); expiresStr != "" {
		if expires, err := http.ParseTime(expiresStr); err == nil {
			lifetime = expires.Sub(responseTime)
		}
	} else if lmStr := headers.Get("Last-Modified"); lmStr != "" {
		if lm, err := http.ParseTime(lmStr); err == nil {
			lifetime = responseTime.Sub(lm) / 10
		}
	}
	if lifetime < 0 {
		lifetime = 0
	}
	return responseTime.Add(lifetime)
}

func findOverrideTTL(relPath string, overrides []config.CacheOverride) (time.Duration, bool) {
	for _, rule := range overrides {
		matched, err := doublestar.Match(rule.PathPattern, relPath)
		if err == nil && matched {
			return rule.TTL, true
		}
	}
	return 0, false
}
