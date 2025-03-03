package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/handlers"
	"github.com/yolkispalkis/go-apt-cache/internal/storage"
	"github.com/yolkispalkis/go-apt-cache/internal/utils"
)

// initializeCache sets up the cache based on configuration
func initializeCache(cfg *config.Config) (storage.Cache, storage.HeaderCache, error) {
	if !cfg.Cache.Enabled {
		log.Println("Cache is disabled, using noop cache")
		return storage.NewNoopCache(), storage.NewNoopHeaderCache(), nil
	}

	cacheDir := cfg.Cache.Directory
	if cacheDir == "" {
		cacheDir = "./cache"
	}

	// Create absolute path if relative
	if !filepath.IsAbs(cacheDir) {
		absPath, err := filepath.Abs(cacheDir)
		if err == nil {
			cacheDir = absPath
		}
	}

	// Ensure cache directory exists with proper error handling
	log.Printf("Creating cache directory at %s", cacheDir)
	if err := utils.CreateDirectory(cacheDir); err != nil {
		return nil, nil, fmt.Errorf("failed to create cache directory: %v", err)
	}

	// Convert cache size based on unit
	maxSizeBytes := utils.ConvertSizeToBytes(cfg.Cache.MaxSize, cfg.Cache.SizeUnit)

	var cache storage.Cache
	var headerCache storage.HeaderCache
	var err error

	// Always use LRU cache if cache is enabled (removed the LRU flag check)
	// Create LRU cache with options
	lruOptions := storage.LRUCacheOptions{
		BasePath:     cacheDir,
		MaxSizeBytes: maxSizeBytes,
		CleanOnStart: cfg.Cache.CleanOnStart,
	}

	cache, err = storage.NewLRUCacheWithOptions(lruOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize LRU cache: %v", err)
	}

	// Get cache stats after initialization
	if lruCache, ok := cache.(*storage.LRUCache); ok {
		itemCount, currentSize, maxSize := lruCache.GetCacheStats()
		log.Printf("LRU cache initialized with %d items, current size: %d bytes, max size: %d bytes",
			itemCount, currentSize, maxSize)
	}

	log.Printf("Using LRU disk cache at %s (max size: %d %s)", cacheDir, cfg.Cache.MaxSize, cfg.Cache.SizeUnit)

	// Initialize header cache
	headerCache, err = storage.NewFileHeaderCache(cacheDir)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize header cache: %v", err)
	}
	log.Printf("Using header cache at %s", cacheDir)

	return cache, headerCache, nil
}

// setupHTTPServer creates and configures the HTTP server
func setupHTTPServer(cfg *config.Config, cache storage.Cache, headerCache storage.HeaderCache, httpClient *http.Client) *http.Server {
	mux := http.NewServeMux()

	// Register handlers for each repository
	for _, repo := range cfg.Repositories {
		if !repo.Enabled {
			log.Printf("Skipping disabled repository: %s", repo.Origin)
			continue
		}

		origin := utils.NormalizeOriginURL(repo.Origin)
		basePath := utils.NormalizeBasePath(repo.Path)

		log.Printf("Setting up mirror for %s at path %s", origin, basePath)

		// Create handler config
		handlerConfig := handlers.ServerConfig{
			OriginServer: origin,
			Cache:        cache,
			HeaderCache:  headerCache,
			LogRequests:  cfg.Server.LogRequests,
			Client:       httpClient,
		}

		// Register handlers
		releaseHandler := handlers.HandleRelease(handlerConfig)
		cacheableHandler := handlers.HandleCacheableRequest(handlerConfig)

		// Register paths
		mux.HandleFunc(basePath+"dists/", releaseHandler)
		mux.HandleFunc(basePath+"pool/", cacheableHandler)
		mux.HandleFunc(basePath, cacheableHandler) // Handle root path
	}

	// Add status endpoint
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok","version":"1.0.0"}`))
	})

	// Create a middleware to handle reverse proxy headers
	reverseProxyMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Get real IP from X-Forwarded-For or X-Real-IP headers
			if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
				// X-Forwarded-For can contain multiple IPs, use the first one
				ips := strings.Split(xff, ",")
				if len(ips) > 0 {
					r.RemoteAddr = strings.TrimSpace(ips[0])
				}
			} else if xrip := r.Header.Get("X-Real-IP"); xrip != "" {
				r.RemoteAddr = xrip
			}

			// Handle X-Forwarded-Proto for proper scheme detection
			if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
				if proto == "https" {
					r.TLS = &tls.ConnectionState{} // Fake TLS connection
				}
			}

			next.ServeHTTP(w, r)
		})
	}

	// Create server with optimized settings for high traffic
	server := &http.Server{
		Addr:         cfg.Server.ListenAddress,
		Handler:      reverseProxyMiddleware(mux),
		ReadTimeout:  120 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  180 * time.Second,
		// Optimize for high concurrency
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	return server
}

// applyCommandLineFlags applies command line flags to the configuration
func applyCommandLineFlags(cfg *config.Config, flags map[string]interface{}) {
	// Add repository if specified
	if originServer, ok := flags["origin"].(string); ok && originServer != "" {
		// Check if repository already exists
		found := false
		for _, repo := range cfg.Repositories {
			if repo.Origin == originServer {
				found = true
				break
			}
		}

		if !found {
			cfg.Repositories = append(cfg.Repositories, config.Repository{
				Origin:  originServer,
				Path:    "/",
				Enabled: true,
			})
		}
	}

	// Override server settings
	if port, ok := flags["port"].(int); ok && port != 0 {
		cfg.Server.ListenAddress = fmt.Sprintf("%s:%d",
			strings.Split(cfg.Server.ListenAddress, ":")[0], port)
	}

	if bindAddress, ok := flags["bind"].(string); ok && bindAddress != "" {
		parts := strings.Split(cfg.Server.ListenAddress, ":")
		if len(parts) > 1 {
			cfg.Server.ListenAddress = fmt.Sprintf("%s:%s", bindAddress, parts[1])
		} else {
			cfg.Server.ListenAddress = fmt.Sprintf("%s:8080", bindAddress)
		}
	}

	// Override timeout setting
	if timeout, ok := flags["timeout"].(int); ok && timeout > 0 {
		cfg.Server.Timeout = timeout
	}

	// Override cache settings
	if logRequests, ok := flags["log-requests"].(bool); ok {
		cfg.Server.LogRequests = logRequests
	}

	if disableCache, ok := flags["disable-cache"].(bool); ok && disableCache {
		cfg.Cache.Enabled = false
	}

	if cacheDir, ok := flags["cache-dir"].(string); ok && cacheDir != "" {
		cfg.Cache.Directory = cacheDir
	}

	if cacheSize, ok := flags["cache-size"].(int64); ok && cacheSize > 0 {
		cfg.Cache.MaxSize = cacheSize
	}

	if cacheSizeUnit, ok := flags["cache-size-unit"].(string); ok && cacheSizeUnit != "" {
		cfg.Cache.SizeUnit = cacheSizeUnit
	}

	// LRU flag is now ignored as we always use LRU cache when cache is enabled

	if cleanCache, ok := flags["clean-cache"].(bool); ok {
		cfg.Cache.CleanOnStart = cleanCache
	}
}

func main() {
	// Parse command line flags
	configFile := flag.String("config", "config.json", "Path to configuration file")
	createConfigFlag := flag.Bool("create-config", false, "Create default configuration file if it doesn't exist")
	originServer := flag.String("origin", "", "Origin server to mirror (e.g. archive.ubuntu.com)")
	port := flag.Int("port", 0, "Port to listen on (overrides config file)")
	bindAddress := flag.String("bind", "", "Address to bind to (overrides config file)")
	logRequests := flag.Bool("log-requests", true, "Log all requests (overrides config file)")
	disableCache := flag.Bool("disable-cache", false, "Disable caching (overrides config file)")
	cacheDir := flag.String("cache-dir", "", "Directory to store cache files (overrides config file)")
	cacheSize := flag.Int64("cache-size", 0, "Maximum cache size (overrides config file)")
	cacheSizeUnit := flag.String("cache-size-unit", "", "Cache size unit: bytes, MB, GB, or TB (overrides config file)")
	cleanCache := flag.Bool("clean-cache", false, "Clean cache on startup (overrides config file)")
	timeoutSeconds := flag.Int("timeout", 60, "Timeout in seconds for HTTP requests to origin servers")
	flag.Parse()

	// Create default config file if requested
	if *createConfigFlag {
		if err := config.CreateDefaultConfigFile(*configFile); err != nil {
			if !os.IsExist(err) {
				log.Fatalf("Failed to create config file: %v", err)
			}
			log.Printf("Config file already exists at %s", *configFile)
		} else {
			log.Printf("Created default config file at %s", *configFile)
			if !flag.Parsed() {
				return
			}
		}
	}

	// Load configuration
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		log.Printf("Error loading config: %v", err)
		log.Println("Using default configuration")
		cfg = config.DefaultConfig()
	}

	// Apply command line flags to configuration
	flags := map[string]interface{}{
		"origin":          *originServer,
		"port":            *port,
		"bind":            *bindAddress,
		"log-requests":    *logRequests,
		"disable-cache":   *disableCache,
		"cache-dir":       *cacheDir,
		"cache-size":      *cacheSize,
		"cache-size-unit": *cacheSizeUnit,
		"clean-cache":     *cleanCache,
		"timeout":         *timeoutSeconds,
	}
	applyCommandLineFlags(&cfg, flags)

	// Validate configuration
	if cfg.Server.ListenAddress == "" {
		cfg.Server.ListenAddress = ":8080"
	}

	if len(cfg.Repositories) == 0 {
		log.Fatal("No repositories configured. Use --origin flag or add repositories to the config file.")
	}

	// Initialize cache
	cache, headerCache, err := initializeCache(&cfg)
	if err != nil {
		log.Fatalf("Failed to initialize cache: %v", err)
	}

	// Create custom HTTP client with timeout for origin server requests
	httpClient := utils.CreateHTTPClient(cfg.Server.Timeout)

	// Set up HTTP server
	server := setupHTTPServer(&cfg, cache, headerCache, httpClient)

	// Set up graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Start server in a goroutine
	go func() {
		log.Printf("Server listening on %s", cfg.Server.ListenAddress)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Error starting server: %v", err)
		}
	}()

	// Wait for interrupt signal
	<-stop
	log.Println("Shutting down server...")

	// Create context with timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Attempt graceful shutdown
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server gracefully stopped")
}
