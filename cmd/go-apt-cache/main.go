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

// CacheInitializer encapsulates cache initialization logic
type CacheInitializer struct {
	Config *config.Config
}

// Initialize sets up the cache based on configuration
func (ci *CacheInitializer) Initialize() (storage.Cache, storage.HeaderCache, storage.ValidationCache, error) {
	cfg := ci.Config

	if !cfg.Cache.Enabled {
		log.Println("Cache is disabled, using noop cache")
		return storage.NewNoopCache(), storage.NewNoopHeaderCache(), storage.NewNoopValidationCache(), nil
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
		return nil, nil, nil, fmt.Errorf("failed to create cache directory: %v", err)
	}

	// Convert cache size based on unit
	maxSizeBytes := utils.ConvertSizeToBytes(cfg.Cache.MaxSize, cfg.Cache.SizeUnit)

	// Create LRU cache with options
	lruOptions := storage.LRUCacheOptions{
		BasePath:     cacheDir,
		MaxSizeBytes: maxSizeBytes,
		CleanOnStart: cfg.Cache.CleanOnStart,
	}

	lruCache, err := storage.NewLRUCacheWithOptions(lruOptions)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to initialize LRU cache: %v", err)
	}

	// Get cache stats after initialization
	itemCount, currentSize, maxSize := lruCache.GetCacheStats()
	log.Printf("LRU cache initialized with %d items, current size: %d bytes, max size: %d bytes",
		itemCount, currentSize, maxSize)

	log.Printf("Using LRU disk cache at %s (max size: %d %s)", cacheDir, cfg.Cache.MaxSize, cfg.Cache.SizeUnit)

	// Initialize header cache
	headerCache, err := storage.NewFileHeaderCache(cacheDir)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to initialize header cache: %v", err)
	}
	log.Printf("Using header cache at %s", cacheDir)

	// Initialize validation cache
	validationTTL := time.Duration(cfg.Cache.ValidationCacheTTL) * time.Second
	validationCache := storage.NewMemoryValidationCache(validationTTL)
	log.Printf("Using in-memory validation cache with TTL of %v", validationTTL)

	// Return the LRUCache as a Cache interface
	return lruCache, headerCache, validationCache, nil
}

// ServerSetup encapsulates server setup logic
type ServerSetup struct {
	Config          *config.Config
	Cache           storage.Cache
	HeaderCache     storage.HeaderCache
	ValidationCache storage.ValidationCache
	HTTPClient      *http.Client
}

// CreateServer creates and configures the HTTP server
func (ss *ServerSetup) CreateServer() *http.Server {
	mux := http.NewServeMux()

	// Register handlers for each repository
	ss.registerRepositoryHandlers(mux)

	// Add status endpoint
	mux.HandleFunc("/status", ss.handleStatus)

	// Create server with optimized settings for high traffic
	server := &http.Server{
		Addr:         ss.Config.Server.ListenAddress,
		Handler:      ss.createReverseProxyMiddleware(mux),
		ReadTimeout:  120 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  180 * time.Second,
		// Optimize for high concurrency
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	return server
}

// registerRepositoryHandlers registers handlers for each repository
func (ss *ServerSetup) registerRepositoryHandlers(mux *http.ServeMux) {
	for _, repo := range ss.Config.Repositories {
		if !repo.Enabled {
			log.Printf("Skipping disabled repository: %s", repo.URL)
			continue
		}

		originURL := utils.NormalizeOriginURL(repo.URL)
		basePath := utils.NormalizeBasePath(repo.Path)

		log.Printf("Setting up mirror for %s at path %s", originURL, basePath)

		// Create handler config
		handlerConfig := handlers.ServerConfig{
			OriginServer:    originURL,
			Cache:           ss.Cache,
			HeaderCache:     ss.HeaderCache,
			ValidationCache: ss.ValidationCache,
			LogRequests:     ss.Config.Server.LogRequests,
			Client:          ss.HTTPClient,
			LocalPath:       basePath, // Add local path for proper URL mapping
		}

		// Register handlers
		releaseHandler := handlers.HandleRelease(handlerConfig)
		cacheableHandler := handlers.HandleCacheableRequest(handlerConfig)

		// Register paths
		mux.HandleFunc(basePath+"dists/", releaseHandler)
		mux.HandleFunc(basePath+"pool/", cacheableHandler)
		mux.HandleFunc(basePath, cacheableHandler) // Handle root path
	}
}

// handleStatus handles the status endpoint
func (ss *ServerSetup) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok","version":"1.0.0"}`))
}

// createReverseProxyMiddleware creates a middleware to handle reverse proxy headers
func (ss *ServerSetup) createReverseProxyMiddleware(next http.Handler) http.Handler {
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

// ConfigManager handles configuration loading and command line flags
type ConfigManager struct {
	ConfigFile       string
	CreateConfigFlag bool
	CommandLineFlags map[string]interface{}
}

// NewConfigManager creates a new ConfigManager with parsed command line flags
func NewConfigManager() *ConfigManager {
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

	// Create flags map
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

	return &ConfigManager{
		ConfigFile:       *configFile,
		CreateConfigFlag: *createConfigFlag,
		CommandLineFlags: flags,
	}
}

// LoadConfig loads the configuration from file and applies command line flags
func (cm *ConfigManager) LoadConfig() (config.Config, error) {
	// Create default config file if requested
	if cm.CreateConfigFlag {
		if err := config.CreateDefaultConfigFile(cm.ConfigFile); err != nil {
			if !os.IsExist(err) {
				return config.Config{}, fmt.Errorf("failed to create config file: %v", err)
			}
			log.Printf("Config file already exists at %s", cm.ConfigFile)
		} else {
			log.Printf("Created default config file at %s", cm.ConfigFile)
			return config.DefaultConfig(), nil
		}
	}

	// Load configuration
	cfg, err := config.LoadConfig(cm.ConfigFile)
	if err != nil {
		log.Printf("Error loading config: %v", err)
		log.Println("Using default configuration")
		cfg = config.DefaultConfig()
	}

	// Apply command line flags to configuration
	cm.applyCommandLineFlags(&cfg)

	// Validate configuration
	if cfg.Server.ListenAddress == "" {
		cfg.Server.ListenAddress = ":8080"
	}

	if len(cfg.Repositories) == 0 {
		return cfg, fmt.Errorf("no repositories configured. Use --origin flag or add repositories to the config file")
	}

	return cfg, nil
}

// applyCommandLineFlags applies command line flags to the configuration
func (cm *ConfigManager) applyCommandLineFlags(cfg *config.Config) {
	flags := cm.CommandLineFlags

	// Add repository if specified
	if originServer, ok := flags["origin"].(string); ok && originServer != "" {
		// Check if repository already exists
		found := false
		for _, repo := range cfg.Repositories {
			if repo.URL == originServer {
				found = true
				break
			}
		}

		if !found {
			cfg.Repositories = append(cfg.Repositories, config.Repository{
				URL:     originServer,
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

	if cleanCache, ok := flags["clean-cache"].(bool); ok {
		cfg.Cache.CleanOnStart = cleanCache
	}
}

// ServerManager handles server lifecycle
type ServerManager struct {
	Server *http.Server
}

// StartServer starts the server and handles graceful shutdown
func (sm *ServerManager) StartServer() error {
	// Set up graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Start server in a goroutine
	go func() {
		log.Printf("Server listening on %s", sm.Server.Addr)
		if err := sm.Server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
	if err := sm.Server.Shutdown(ctx); err != nil {
		return fmt.Errorf("server forced to shutdown: %v", err)
	}

	log.Println("Server gracefully stopped")
	return nil
}

func main() {
	// Initialize configuration
	configManager := NewConfigManager()
	cfg, err := configManager.LoadConfig()
	if err != nil {
		log.Fatal(err)
	}

	// Initialize cache
	cacheInitializer := &CacheInitializer{Config: &cfg}
	cache, headerCache, validationCache, err := cacheInitializer.Initialize()
	if err != nil {
		log.Fatalf("Failed to initialize cache: %v", err)
	}

	// Create custom HTTP client with timeout for origin server requests
	httpClient := utils.CreateHTTPClient(cfg.Server.Timeout)

	// Set up HTTP server
	serverSetup := &ServerSetup{
		Config:          &cfg,
		Cache:           cache,
		HeaderCache:     headerCache,
		ValidationCache: validationCache,
		HTTPClient:      httpClient,
	}
	server := serverSetup.CreateServer()

	// Start server and handle lifecycle
	serverManager := &ServerManager{Server: server}
	if err := serverManager.StartServer(); err != nil {
		log.Fatal(err)
	}
}
