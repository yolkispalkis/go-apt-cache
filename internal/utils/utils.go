package utils

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// CreateDirectory ensures a directory exists, handling common issues on different platforms
func CreateDirectory(path string) error {
	// First attempt to create the directory
	if err := os.MkdirAll(path, 0755); err != nil {
		log.Printf("Error creating directory %s: %v", path, err)

		// Try the component-by-component approach for Windows
		if runtime.GOOS == "windows" {
			// Split the path into components
			components := strings.Split(filepath.ToSlash(path), "/")
			currentPath := components[0]

			// On Windows, the first component might be empty for absolute paths
			if currentPath == "" && len(components) > 1 {
				currentPath = components[1]
				components = components[2:]
			} else {
				components = components[1:]
			}

			// Add drive letter back for Windows
			if !strings.HasSuffix(currentPath, ":") {
				// Check if we need to add the drive letter
				if strings.Contains(path, ":") {
					driveLetter := strings.Split(path, ":")[0]
					currentPath = driveLetter + ":"
				}
			}

			// Create each directory component
			for _, component := range components {
				if component == "" {
					continue
				}

				currentPath = filepath.Join(currentPath, component)
				err = os.Mkdir(currentPath, 0755)
				if err != nil && !os.IsExist(err) {
					// Check if the path exists but is a file
					info, statErr := os.Stat(currentPath)
					if statErr == nil && !info.IsDir() {
						// It's a file, try to remove it and create directory
						if removeErr := os.Remove(currentPath); removeErr != nil {
							return fmt.Errorf("failed to remove file at directory path: %w", removeErr)
						}
						if mkdirErr := os.Mkdir(currentPath, 0755); mkdirErr != nil {
							return fmt.Errorf("failed to create directory after removing file: %w", mkdirErr)
						}
					} else {
						return fmt.Errorf("failed to create directory component %s: %w", currentPath, err)
					}
				}
			}

			return nil
		}

		return err
	}

	// Verify the directory was created and is actually a directory
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Try again with a different approach
			if err := os.MkdirAll(path, 0755); err != nil {
				return fmt.Errorf("failed to create directory on second attempt: %w", err)
			}
			return nil
		}
		return fmt.Errorf("error checking directory: %w", err)
	}

	// If path exists but is not a directory, try to handle it
	if !info.IsDir() {
		log.Printf("Path exists but is not a directory: %s", path)
		// Try to remove the file and create directory
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("failed to remove file at directory path: %w", err)
		}
		if err := os.MkdirAll(path, 0755); err != nil {
			return fmt.Errorf("failed to create directory after removing file: %w", err)
		}
	}

	return nil
}

// ConvertSizeToBytes converts a size value from a specified unit to bytes
func ConvertSizeToBytes(size int64, unit string) int64 {
	switch strings.ToUpper(unit) {
	case "MB":
		return size * 1024 * 1024
	case "GB":
		return size * 1024 * 1024 * 1024
	case "TB":
		return size * 1024 * 1024 * 1024 * 1024
	case "BYTES", "":
		// No conversion needed
		return size
	default:
		log.Printf("Warning: Unknown size unit '%s', using bytes", unit)
		return size
	}
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
	// This will use HTTP_PROXY, HTTPS_PROXY, and NO_PROXY environment variables
	proxyFunc := http.ProxyFromEnvironment
	transport.Proxy = proxyFunc

	// Log if proxy is configured
	httpProxy := os.Getenv("HTTP_PROXY")
	httpsProxy := os.Getenv("HTTPS_PROXY")
	noProxy := os.Getenv("NO_PROXY")

	if httpProxy != "" || httpsProxy != "" {
		log.Printf("Using proxy configuration from environment variables")
		if httpProxy != "" {
			log.Printf("HTTP_PROXY domain: %s", GetProxyDomain(httpProxy))
		}
		if httpsProxy != "" {
			log.Printf("HTTPS_PROXY domain: %s", GetProxyDomain(httpsProxy))
		}
		if noProxy != "" {
			log.Printf("NO_PROXY: %s", noProxy)
		}
	}

	return &http.Client{
		Timeout:   time.Duration(timeoutSeconds) * time.Second,
		Transport: transport,
	}
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
	ext := strings.ToLower(filepath.Ext(path))

	// Check for known content types
	for _, mapping := range contentTypes {
		for _, extension := range mapping.Extensions {
			if ext == extension {
				return mapping.MIMEType
			}
		}
	}

	// Default to binary for unknown types
	return "application/octet-stream"
}

// GetProxyDomain extracts only the domain part from a proxy URL
// Example: http://user:pass@proxy.example.com:8080 -> proxy.example.com
func GetProxyDomain(proxyURL string) string {
	// Remove protocol part if present
	domainPart := proxyURL
	if strings.Contains(proxyURL, "://") {
		parts := strings.SplitN(proxyURL, "://", 2)
		if len(parts) == 2 {
			domainPart = parts[1]
		}
	}

	// Remove user:password part if present
	if strings.Contains(domainPart, "@") {
		parts := strings.SplitN(domainPart, "@", 2)
		if len(parts) == 2 {
			domainPart = parts[1]
		}
	}

	// Remove port if present
	if strings.Contains(domainPart, ":") {
		parts := strings.SplitN(domainPart, ":", 2)
		if len(parts) == 2 {
			domainPart = parts[0]
		}
	}

	// Remove path if present
	if strings.Contains(domainPart, "/") {
		parts := strings.SplitN(domainPart, "/", 2)
		if len(parts) == 2 {
			domainPart = parts[0]
		}
	}

	return domainPart
}
