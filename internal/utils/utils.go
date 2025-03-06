package utils

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/logging"
)

func CreateDirectory(path string) error {
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", path, err)
	}

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to verify directory creation: %w", err)
	}

	if !info.IsDir() {
		return fmt.Errorf("%s exists but is not a directory", path)
	}

	return nil
}

func CreateHTTPClient(timeoutSeconds int) *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 200,
		MaxConnsPerHost:     500,
		IdleConnTimeout:     120 * time.Second,
		DisableCompression:  false,
		ForceAttemptHTTP2:   true,
		TLSHandshakeTimeout: 10 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 60 * time.Second,
			DualStack: true,
		}).DialContext,
		DisableKeepAlives:     false,
		ResponseHeaderTimeout: 30 * time.Second,
		WriteBufferSize:       64 * 1024,
		ReadBufferSize:        64 * 1024,
	}

	proxyFunc := http.ProxyFromEnvironment
	transport.Proxy = proxyFunc

	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration(timeoutSeconds) * time.Second,
	}

	return client
}

func CreateHTTPClientWithProxy(timeoutSeconds int, proxyURL string) *http.Client {
	client := CreateHTTPClient(timeoutSeconds)

	if proxyURL != "" {
		parsedURL, err := url.Parse(proxyURL)
		if err == nil {
			if transport, ok := client.Transport.(*http.Transport); ok {
				transport.Proxy = http.ProxyURL(parsedURL)
			}
		}
	}

	return client
}

func NormalizeBasePath(basePath string) string {
	if basePath == "" {
		return "/"
	}

	if !strings.HasPrefix(basePath, "/") {
		basePath = "/" + basePath
	}

	if !strings.HasSuffix(basePath, "/") {
		basePath = basePath + "/"
	}

	return basePath
}

func NormalizeURL(url string) string {
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}

	url = strings.TrimSuffix(url, "/")

	return url
}

type FileType int

const (
	TypeFrequentlyChanging FileType = iota
	TypeRarelyChanging
)

type FilePattern struct {
	Pattern string
	Type    FileType
}

type ContentTypeMapping struct {
	Extensions []string
	MIMEType   string
}

var (
	filePatterns = []FilePattern{
		{Pattern: "InRelease", Type: TypeFrequentlyChanging},
		{Pattern: "Release.gpg", Type: TypeFrequentlyChanging},
		{Pattern: "/Release", Type: TypeFrequentlyChanging},
		{Pattern: "ls-lR.gz", Type: TypeFrequentlyChanging},
		{Pattern: "by-hash", Type: TypeFrequentlyChanging},
		{Pattern: "Translation-", Type: TypeFrequentlyChanging},
		{Pattern: "Components-", Type: TypeFrequentlyChanging},
		{Pattern: "Packages", Type: TypeFrequentlyChanging},
		{Pattern: "Packages.gz", Type: TypeFrequentlyChanging},
		{Pattern: "Packages.xz", Type: TypeFrequentlyChanging},
		{Pattern: "Packages.bz2", Type: TypeFrequentlyChanging},
		{Pattern: "Sources", Type: TypeFrequentlyChanging},
		{Pattern: "Sources.gz", Type: TypeFrequentlyChanging},
		{Pattern: "Sources.xz", Type: TypeFrequentlyChanging},
		{Pattern: "Sources.bz2", Type: TypeFrequentlyChanging},
		{Pattern: "Contents-", Type: TypeFrequentlyChanging},
		{Pattern: "Index", Type: TypeFrequentlyChanging},
		{Pattern: "i18n", Type: TypeFrequentlyChanging},
		{Pattern: "dep11", Type: TypeFrequentlyChanging},
		{Pattern: "icons-", Type: TypeFrequentlyChanging},

		{Pattern: ".deb", Type: TypeRarelyChanging},
		{Pattern: ".udeb", Type: TypeRarelyChanging},
		{Pattern: ".dsc", Type: TypeRarelyChanging},
		{Pattern: ".tar.gz", Type: TypeRarelyChanging},
		{Pattern: ".tar.xz", Type: TypeRarelyChanging},
		{Pattern: ".tar.bz2", Type: TypeRarelyChanging},
		{Pattern: ".diff.gz", Type: TypeRarelyChanging},
		{Pattern: ".changes", Type: TypeRarelyChanging},
	}

	contentTypes = []ContentTypeMapping{
		{Extensions: []string{".gz", ".gzip"}, MIMEType: "application/gzip"},
		{Extensions: []string{".bz2"}, MIMEType: "application/x-bzip2"},
		{Extensions: []string{".xz"}, MIMEType: "application/x-xz"},
		{Extensions: []string{".deb", ".udeb"}, MIMEType: "application/vnd.debian.binary-package"},
		{Extensions: []string{".asc"}, MIMEType: "application/pgp-signature"},
		{Extensions: []string{".gpg"}, MIMEType: "application/pgp-encrypted"},
		{Extensions: []string{".json"}, MIMEType: "application/json"},
		{Extensions: []string{".xml"}, MIMEType: "application/xml"},
		{Extensions: []string{".txt", ".list"}, MIMEType: "text/plain"},
		{Extensions: []string{".html", ".htm"}, MIMEType: "text/html"},
		{Extensions: []string{".dsc"}, MIMEType: "text/x-dsc"},
		{Extensions: []string{".changes"}, MIMEType: "text/x-changes"},
		{Extensions: []string{".diff"}, MIMEType: "text/x-diff"},
		{Extensions: []string{".patch"}, MIMEType: "text/x-patch"},
		{Extensions: []string{".tar"}, MIMEType: "application/x-tar"},
		{Extensions: []string{".yaml", ".yml"}, MIMEType: "application/yaml"},
		{Extensions: []string{".sig"}, MIMEType: "application/pgp-signature"},
		{Extensions: []string{".deb.asc", ".udeb.asc"}, MIMEType: "application/pgp-signature"},
		{Extensions: []string{".tar.asc", ".tar.gz.asc", ".tar.xz.asc"}, MIMEType: "application/pgp-signature"},
		{Extensions: []string{".deb.sig", ".udeb.sig"}, MIMEType: "application/pgp-signature"},
		{Extensions: []string{".tar.sig", ".tar.gz.sig", ".tar.xz.sig"}, MIMEType: "application/pgp-signature"},
	}
)

func GetFilePatternType(path string) FileType {
	normalizedPath := filepath.ToSlash(path)

	if strings.HasSuffix(normalizedPath, "/") {
		return TypeFrequentlyChanging
	}

	for _, pattern := range filePatterns {
		if strings.Contains(normalizedPath, pattern.Pattern) {
			return pattern.Type
		}
	}

	switch {
	case strings.Contains(normalizedPath, "/dists/"):
		return TypeFrequentlyChanging
	case strings.Contains(normalizedPath, "/pool/"):
		return TypeRarelyChanging
	default:
		return TypeRarelyChanging
	}
}

func GetContentType(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	if ext == "" {
		logging.Warning("Could not determine content type for: %s, no extension", path)
		return "application/octet-stream"
	}

	ext = ext[1:]

	for _, mapping := range contentTypes {
		for _, extension := range mapping.Extensions {
			if extension == ext {
				return mapping.MIMEType
			}
		}
	}
	logging.Warning("Could not determine content type for: %s", path)
	return "application/octet-stream"
}

func WrapError(message string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}
