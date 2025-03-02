package storage

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileHeaderCacheJSON(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "header-cache-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a new header cache
	cache, err := NewFileHeaderCache(tempDir)
	if err != nil {
		t.Fatalf("Failed to create header cache: %v", err)
	}

	// Create test headers
	testHeaders := http.Header{}
	testHeaders.Add("Content-Type", "application/json")
	testHeaders.Add("Content-Length", "1024")
	testHeaders.Add("X-Test-Header", "test value")
	testHeaders.Add("X-Test-Header", "another value") // Multiple values for the same key

	// Test key
	testKey := "/path/to/test/file.json"

	// Store headers
	if err := cache.PutHeaders(testKey, testHeaders); err != nil {
		t.Fatalf("Failed to store headers: %v", err)
	}

	// Retrieve headers
	retrievedHeaders, err := cache.GetHeaders(testKey)
	if err != nil {
		t.Fatalf("Failed to retrieve headers: %v", err)
	}

	// Verify headers
	for key, values := range testHeaders {
		retrievedValues := retrievedHeaders[key]
		if len(retrievedValues) != len(values) {
			t.Errorf("Expected %d values for header %s, got %d", len(values), key, len(retrievedValues))
		}

		for i, value := range values {
			if i >= len(retrievedValues) || retrievedValues[i] != value {
				t.Errorf("Expected header %s to have value %s at index %d, got %v", key, value, i, retrievedValues)
			}
		}
	}

	// Verify the file exists and contains JSON
	filename := filepath.Join(tempDir, filepath.FromSlash(testKey)) + ".headercache"
	content, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read header file: %v", err)
	}

	// Check if the content starts with a JSON object marker
	if len(content) == 0 || content[0] != '{' {
		t.Errorf("Header file does not contain JSON: %s", content)
	}

	t.Logf("Header file content: %s", content)
}

func TestFileHeaderCache(t *testing.T) {
	t.Log("Starting header cache test")

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "header-cache-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a new header cache
	cache, err := NewFileHeaderCache(tempDir)
	if err != nil {
		t.Fatalf("Failed to create header cache: %v", err)
	}

	// Test with a key that includes path components
	testKey := "path/to/test-file.txt"
	testHeaders := http.Header{}
	testHeaders.Add("Content-Type", "text/html")
	testHeaders.Add("Content-Length", "1024")
	testHeaders.Add("X-Test-Header", "test value")

	// Store headers
	if err := cache.PutHeaders(testKey, testHeaders); err != nil {
		t.Fatalf("Failed to store headers: %v", err)
	}

	// Verify the file exists
	filename := filepath.Join(tempDir, filepath.FromSlash(testKey)) + ".headercache"
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Fatalf("Header file does not exist: %s", filename)
	}

	// Retrieve headers
	retrievedHeaders, err := cache.GetHeaders(testKey)
	if err != nil {
		t.Fatalf("Failed to retrieve headers: %v", err)
	}

	// Verify headers
	if retrievedHeaders.Get("Content-Type") != "text/html" {
		t.Errorf("Expected Content-Type to be text/html, got %s", retrievedHeaders.Get("Content-Type"))
	}
	if retrievedHeaders.Get("Content-Length") != "1024" {
		t.Errorf("Expected Content-Length to be 1024, got %s", retrievedHeaders.Get("Content-Length"))
	}
	if retrievedHeaders.Get("X-Test-Header") != "test value" {
		t.Errorf("Expected X-Test-Header to be test value, got %s", retrievedHeaders.Get("X-Test-Header"))
	}

	t.Log("Header cache test passed")
}

func TestHierarchicalDirectoryStructure(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "hierarchical-cache-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a new LRU cache
	cache, err := NewLRUCache(tempDir, 1024*1024*10) // 10MB cache
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Test with a key that includes path components
	testKey := "dists/focal/main/binary-amd64/Packages"
	testContent := []byte("This is test content for hierarchical directory structure")

	// Store content
	err = cache.Put(testKey,
		io.NopCloser(bytes.NewReader(testContent)),
		int64(len(testContent)),
		time.Now())
	if err != nil {
		t.Fatalf("Failed to store content: %v", err)
	}

	// Verify the directory structure was created
	expectedPath := filepath.Join(tempDir, "dists", "focal", "main", "binary-amd64", "Packages.filecache")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Fatalf("Expected file does not exist at path: %s", expectedPath)
	}

	// Retrieve content
	reader, size, _, err := cache.Get(testKey)
	if err != nil {
		t.Fatalf("Failed to retrieve content: %v", err)
	}
	defer reader.Close()

	// Read content
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read content: %v", err)
	}

	// Verify content
	if string(content) != string(testContent) {
		t.Errorf("Expected content %s, got %s", string(testContent), string(content))
	}

	// Verify size
	if size != int64(len(testContent)) {
		t.Errorf("Expected size %d, got %d", len(testContent), size)
	}

	// Test header cache with hierarchical structure
	headerCache, err := NewFileHeaderCache(tempDir)
	if err != nil {
		t.Fatalf("Failed to create header cache: %v", err)
	}

	// Create test headers
	testHeaders := http.Header{}
	testHeaders.Add("Content-Type", "text/plain")
	testHeaders.Add("Content-Length", "42")

	// Store headers
	if err := headerCache.PutHeaders(testKey, testHeaders); err != nil {
		t.Fatalf("Failed to store headers: %v", err)
	}

	// Verify the header file exists
	expectedHeaderPath := filepath.Join(tempDir, "dists", "focal", "main", "binary-amd64", "Packages.headercache")
	if _, err := os.Stat(expectedHeaderPath); os.IsNotExist(err) {
		t.Fatalf("Expected header file does not exist at path: %s", expectedHeaderPath)
	}

	// Retrieve headers
	retrievedHeaders, err := headerCache.GetHeaders(testKey)
	if err != nil {
		t.Fatalf("Failed to retrieve headers: %v", err)
	}

	// Verify headers
	if retrievedHeaders.Get("Content-Type") != "text/plain" {
		t.Errorf("Expected Content-Type to be text/plain, got %s", retrievedHeaders.Get("Content-Type"))
	}
	if retrievedHeaders.Get("Content-Length") != "42" {
		t.Errorf("Expected Content-Length to be 42, got %s", retrievedHeaders.Get("Content-Length"))
	}

	t.Log("Hierarchical directory structure test passed")
}
