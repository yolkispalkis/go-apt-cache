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
	return &http.Client{
		Timeout: time.Duration(timeoutSeconds) * time.Second,
		Transport: &http.Transport{
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
		},
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

// NormalizeOriginURL ensures an origin URL has the correct protocol
func NormalizeOriginURL(origin string) string {
	if !strings.HasPrefix(origin, "http://") && !strings.HasPrefix(origin, "https://") {
		return "http://" + origin
	}
	return origin
}
