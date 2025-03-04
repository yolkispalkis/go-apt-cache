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
)

// CreateDirectory ensures a directory exists
func CreateDirectory(path string) error {
	// Create the directory with proper permissions
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", path, err)
	}

	// Verify the directory was created correctly
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to verify directory creation: %w", err)
	}

	if !info.IsDir() {
		return fmt.Errorf("%s exists but is not a directory", path)
	}

	return nil
}

// CreateHTTPClient creates an HTTP client with optimized settings for high traffic
func CreateHTTPClient(timeoutSeconds int) *http.Client {
	// Create transport with optimized settings
	transport := &http.Transport{
		MaxIdleConns:        500,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     250,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
		ForceAttemptHTTP2:   true,
		TLSHandshakeTimeout: 10 * time.Second,
		// Optimize TCP connections
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		// Enable TCP keepalives
		DisableKeepAlives: false,
	}

	// Configure proxy from environment variables
	proxyFunc := http.ProxyFromEnvironment
	transport.Proxy = proxyFunc

	// Create client with the transport
	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration(timeoutSeconds) * time.Second,
	}

	return client
}

// CreateHTTPClientWithProxy creates an HTTP client with optimized settings and specific proxy configuration
func CreateHTTPClientWithProxy(timeoutSeconds int, proxyURL string) *http.Client {
	// Create the base client
	client := CreateHTTPClient(timeoutSeconds)

	// If proxy URL is provided, configure it
	if proxyURL != "" {
		// Parse the proxy URL
		parsedURL, err := url.Parse(proxyURL)
		if err == nil {
			// Get the transport
			if transport, ok := client.Transport.(*http.Transport); ok {
				// Set the proxy function
				transport.Proxy = http.ProxyURL(parsedURL)
			}
		}
	}

	return client
}

// NormalizeBasePath ensures a base path starts and ends with a slash
func NormalizeBasePath(basePath string) string {
	if basePath == "" {
		return "/"
	}

	// Ensure basePath starts with a slash
	if !strings.HasPrefix(basePath, "/") {
		basePath = "/" + basePath
	}

	// Ensure basePath ends with a slash
	if !strings.HasSuffix(basePath, "/") {
		basePath = basePath + "/"
	}

	return basePath
}

// NormalizeURL ensures a URL has the correct protocol and no trailing slash
func NormalizeURL(url string) string {
	// Add protocol if missing
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}

	// Remove trailing slash if present
	url = strings.TrimSuffix(url, "/")

	return url
}

// FileType represents different types of repository files
type FileType int

const (
	// TypeFrequentlyChanging represents files that change frequently (e.g., metadata)
	TypeFrequentlyChanging FileType = iota
	// TypeRarelyChanging represents package files that rarely change
	TypeRarelyChanging
)

// FilePattern represents a pattern for matching repository files
type FilePattern struct {
	Pattern string
	Type    FileType
}

// ContentTypeMapping represents a mapping between file extensions and MIME types
type ContentTypeMapping struct {
	Extensions []string
	MIMEType   string
}

// Common file patterns in Debian repositories
var (
	filePatterns = []FilePattern{
		// Frequently changing files and critical metadata
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

		// Rarely changing files (lowest priority)
		{Pattern: ".deb", Type: TypeRarelyChanging},
		{Pattern: ".udeb", Type: TypeRarelyChanging},
		{Pattern: ".dsc", Type: TypeRarelyChanging},
		{Pattern: ".tar.gz", Type: TypeRarelyChanging},
		{Pattern: ".tar.xz", Type: TypeRarelyChanging},
		{Pattern: ".tar.bz2", Type: TypeRarelyChanging},
		{Pattern: ".diff.gz", Type: TypeRarelyChanging},
		{Pattern: ".changes", Type: TypeRarelyChanging},
	}

	// contentTypes maps file extensions to their MIME types
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

// MatchesFilePattern checks if a path matches any of the given patterns
func MatchesFilePattern(path string, patterns []string) bool {
	normalizedPath := filepath.ToSlash(path)
	for _, pattern := range patterns {
		if strings.Contains(normalizedPath, pattern) {
			return true
		}
	}
	return false
}

// GetFilePatternType determines the type of file based on its path
func GetFilePatternType(path string) FileType {
	normalizedPath := filepath.ToSlash(path)

	// Check patterns in order of priority
	for _, pattern := range filePatterns {
		if strings.Contains(normalizedPath, pattern.Pattern) {
			return pattern.Type
		}
	}

	// Check directory-based patterns
	switch {
	case strings.Contains(normalizedPath, "/dists/"):
		return TypeFrequentlyChanging
	case strings.Contains(normalizedPath, "/pool/"):
		return TypeRarelyChanging
	default:
		return TypeRarelyChanging
	}
}

// GetContentType determines the content type based on file extension
func GetContentType(path string) string {
	// Get file extension
	ext := strings.ToLower(filepath.Ext(path))
	if ext == "" {
		return "application/octet-stream"
	}

	// Remove the leading dot
	ext = ext[1:]

	// Check for known content types
	for _, mapping := range contentTypes {
		for _, extension := range mapping.Extensions {
			if extension == ext {
				return mapping.MIMEType
			}
		}
	}

	// Default content type
	return "application/octet-stream"
}

// WrapError wraps an error with a message
func WrapError(message string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}
