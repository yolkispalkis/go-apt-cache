package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/handlers"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/storage"
	"github.com/yolkispalkis/go-apt-cache/internal/utils"
)

// CacheInitializer encapsulates cache initialization logic
type CacheInitializer struct {
	Config config.Config
}

// Initialize sets up the cache based on configuration
func (ci *CacheInitializer) Initialize() (storage.Cache, storage.HeaderCache, storage.ValidationCache, error) {
	cfg := ci.Config

	if !cfg.Cache.Enabled {
		logging.Info("Cache is disabled, using noop cache")
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
	if err := utils.CreateDirectory(cacheDir); err != nil {
		return nil, nil, nil, utils.WrapError("failed to create cache directory", err)
	}

	logging.Info("Creating cache directory at %s", cacheDir)

	// Initialize cache based on configuration
	var cache storage.Cache
	var headerCache storage.HeaderCache
	var err error

	// Create LRU cache if enabled
	if cfg.Cache.LRU {
		// Parse size string to bytes
		maxSizeBytes, err := utils.ParseSize(cfg.Cache.MaxSize)
		if err != nil {
			// Default to 1GB if parsing fails
			maxSizeBytes = 1024 * 1024 * 1024
			logging.Warning("Invalid cache max size '%s', defaulting to 1GB", cfg.Cache.MaxSize)
		}

		// Clean cache if configured
		if cfg.Cache.CleanOnStart {
			if err := os.RemoveAll(cacheDir); err != nil {
				return nil, nil, nil, utils.WrapError("failed to clean cache directory", err)
			}
			if err := os.MkdirAll(cacheDir, 0755); err != nil {
				return nil, nil, nil, utils.WrapError("failed to recreate cache directory", err)
			}
		}

		// Create LRU cache
		lruOptions := storage.LRUCacheOptions{
			BasePath:     cacheDir,
			MaxSizeBytes: maxSizeBytes,
			CleanOnStart: cfg.Cache.CleanOnStart,
		}
		lruCache, err := storage.NewLRUCacheWithOptions(lruOptions)
		if err != nil {
			return nil, nil, nil, utils.WrapError("failed to create LRU cache", err)
		}

		// Log cache stats
		itemCount, currentSize, maxSize := lruCache.GetCacheStats()
		logging.Info("LRU cache initialized with %d items, current size: %d bytes, max size: %d bytes",
			itemCount, currentSize, maxSize)
		logging.Info("Using LRU disk cache at %s (max size: %s)", cacheDir, cfg.Cache.MaxSize)

		cache = lruCache
	} else {
		// Create simple disk cache
		cache = storage.NewNoopCache() // Placeholder - implement disk cache if needed
	}

	// Create header cache
	headerCache, err = storage.NewFileHeaderCache(cacheDir)
	if err != nil {
		return nil, nil, nil, utils.WrapError("failed to create header cache", err)
	}
	logging.Info("Using header cache at %s", cacheDir)

	// Create validation cache
	validationTTL := time.Duration(cfg.Cache.ValidationCacheTTL) * time.Second
	validationCache := storage.NewMemoryValidationCache(validationTTL)
	logging.Info("Using in-memory validation cache with TTL of %v", validationTTL)

	return cache, headerCache, validationCache, nil
}

// ServerSetup encapsulates server setup and configuration
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

	// Register repository handlers
	ss.registerRepositoryHandlers(mux)

	// Register status endpoint
	mux.HandleFunc("/status", ss.handleStatus)

	// Create middleware chain using the new framework
	middlewareChain := handlers.CreateMiddlewareChain(ss.Config)
	handler := middlewareChain.Apply(mux)

	// Create server
	server := &http.Server{
		Addr:         ss.Config.Server.ListenAddress,
		Handler:      handler,
		ReadTimeout:  time.Duration(ss.Config.Server.Timeout) * time.Second,
		WriteTimeout: time.Duration(ss.Config.Server.Timeout) * time.Second,
		IdleTimeout:  time.Duration(ss.Config.Server.Timeout*2) * time.Second,
	}

	return server
}

// registerRepositoryHandlers registers handlers for each repository
func (ss *ServerSetup) registerRepositoryHandlers(mux *http.ServeMux) {
	for _, repo := range ss.Config.Repositories {
		if !repo.Enabled {
			logging.Info("Skipping disabled repository: %s", repo.URL)
			continue
		}

		// Normalize paths using utility functions
		basePath := utils.NormalizeBasePath(repo.Path)
		upstreamURL := utils.NormalizeURL(repo.URL) + "/" // Ensure trailing slash for upstream URL

		logging.Info("Setting up mirror for %s at path %s", upstreamURL, basePath)

		// Create handler for this repository
		handler := handlers.NewRepositoryHandler(
			upstreamURL,
			ss.Cache,
			ss.HeaderCache,
			ss.ValidationCache,
			ss.HTTPClient,
			basePath,
		)

		// Register handler for this path
		mux.Handle(basePath, http.StripPrefix(basePath, handler))
	}
}

// handleStatus handles the status endpoint
func (ss *ServerSetup) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

// ConfigManager encapsulates configuration loading logic
type ConfigManager struct {
	ConfigFile       string
	CreateConfigFlag bool
	CommandLineFlags map[string]interface{}
}

// NewConfigManager creates a new ConfigManager
func NewConfigManager() *ConfigManager {
	cm := &ConfigManager{
		CommandLineFlags: make(map[string]interface{}),
	}

	// Define command line flags
	configFile := flag.String("config", "config.json", "Path to configuration file")
	createConfig := flag.Bool("create-config", false, "Create default configuration file if it doesn't exist")
	listenAddr := flag.String("listen", "", "Address to listen on (e.g. :8080)")
	unixSocketPath := flag.String("unix-socket", "", "Path to Unix socket (e.g. /var/run/apt-cache.sock)")
	cacheDir := flag.String("cache-dir", "", "Cache directory")
	cacheSize := flag.String("cache-size", "", "Maximum cache size (e.g. 1GB, 500MB)")
	cacheEnabled := flag.Bool("cache-enabled", true, "Enable cache")
	cacheLRU := flag.Bool("cache-lru", true, "Use LRU cache")
	cacheCleanOnStart := flag.Bool("cache-clean", false, "Clean cache on start")
	logFile := flag.String("log-file", "", "Path to log file")
	disableTerminal := flag.Bool("disable-terminal-log", false, "Disable terminal logging")
	logMaxSize := flag.String("log-max-size", "", "Maximum log file size (e.g. 10MB, 1GB)")
	logLevel := flag.String("log-level", "", "Log level (debug, info, warning, error, fatal)")

	// Parse flags
	flag.Parse()

	// Store flag values
	cm.ConfigFile = *configFile
	cm.CreateConfigFlag = *createConfig
	cm.CommandLineFlags["listenAddr"] = *listenAddr
	cm.CommandLineFlags["unixSocketPath"] = *unixSocketPath
	cm.CommandLineFlags["cacheDir"] = *cacheDir
	cm.CommandLineFlags["cacheSize"] = *cacheSize
	cm.CommandLineFlags["cacheEnabled"] = *cacheEnabled
	cm.CommandLineFlags["cacheLRU"] = *cacheLRU
	cm.CommandLineFlags["cacheCleanOnStart"] = *cacheCleanOnStart
	cm.CommandLineFlags["logFile"] = *logFile
	cm.CommandLineFlags["disableTerminal"] = *disableTerminal
	cm.CommandLineFlags["logMaxSize"] = *logMaxSize
	cm.CommandLineFlags["logLevel"] = *logLevel

	return cm
}

// LoadConfig loads the configuration from file and applies command line flags
func (cm *ConfigManager) LoadConfig() (config.Config, error) {
	// Create default config file if requested
	if cm.CreateConfigFlag {
		if _, err := os.Stat(cm.ConfigFile); os.IsNotExist(err) {
			if err := config.CreateDefaultConfigFile(cm.ConfigFile); err != nil {
				return config.DefaultConfig(), fmt.Errorf("failed to create config file: %w", err)
			}
			logging.Info("Created default config file at %s", cm.ConfigFile)
		} else {
			logging.Info("Config file already exists at %s", cm.ConfigFile)
		}
	}

	// Load config from file
	cfg, err := config.LoadConfig(cm.ConfigFile)
	if err != nil {
		logging.Warning("Error loading config: %v", err)
		logging.Info("Using default configuration")
		cfg = config.DefaultConfig()
	}

	// Apply command line flags
	cm.applyCommandLineFlags(&cfg)

	// Validate config
	if err := config.ValidateConfig(cfg); err != nil {
		return cfg, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// applyCommandLineFlags applies command line flags to the configuration
func (cm *ConfigManager) applyCommandLineFlags(cfg *config.Config) {
	// Apply server flags
	if listenAddr, ok := cm.CommandLineFlags["listenAddr"].(string); ok && listenAddr != "" {
		cfg.Server.ListenAddress = listenAddr
	}

	if unixSocketPath, ok := cm.CommandLineFlags["unixSocketPath"].(string); ok && unixSocketPath != "" {
		cfg.Server.UnixSocketPath = unixSocketPath
	}

	// Apply cache flags
	if cacheDir, ok := cm.CommandLineFlags["cacheDir"].(string); ok && cacheDir != "" {
		cfg.Cache.Directory = cacheDir
	}

	if cacheSize, ok := cm.CommandLineFlags["cacheSize"].(string); ok && cacheSize != "" {
		cfg.Cache.MaxSize = cacheSize
	}

	if cacheEnabled, ok := cm.CommandLineFlags["cacheEnabled"].(bool); ok && !cacheEnabled {
		cfg.Cache.Enabled = false
	}

	if cacheLRU, ok := cm.CommandLineFlags["cacheLRU"].(bool); ok && !cacheLRU {
		cfg.Cache.LRU = false
	}

	if cacheCleanOnStart, ok := cm.CommandLineFlags["cacheCleanOnStart"].(bool); ok && cacheCleanOnStart {
		cfg.Cache.CleanOnStart = true
	}

	// Apply logging flags
	if logFile, ok := cm.CommandLineFlags["logFile"].(string); ok && logFile != "" {
		cfg.Logging.FilePath = logFile
	}

	if disableTerminal, ok := cm.CommandLineFlags["disableTerminal"].(bool); ok {
		cfg.Logging.DisableTerminal = disableTerminal
	}

	if logMaxSize, ok := cm.CommandLineFlags["logMaxSize"].(string); ok && logMaxSize != "" {
		cfg.Logging.MaxSize = logMaxSize
	}

	if logLevel, ok := cm.CommandLineFlags["logLevel"].(string); ok && logLevel != "" {
		cfg.Logging.Level = logLevel
	}
}

// ServerManager encapsulates server management logic
type ServerManager struct {
	Server *http.Server
}

// setupUnixSocket configures and starts a Unix socket listener for the server
func setupUnixSocket(server *http.Server, socketPath string, serverError chan<- error) (net.Listener, error) {
	// Remove socket file if it already exists
	if _, err := os.Stat(socketPath); err == nil {
		if err := os.Remove(socketPath); err != nil {
			return nil, fmt.Errorf("failed to remove existing socket file: %w", err)
		}
	}

	// Create Unix socket listener
	unixListener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create Unix socket listener: %w", err)
	}

	// Set permissions on socket file
	if err := os.Chmod(socketPath, 0666); err != nil {
		// Close the listener if we can't set permissions
		unixListener.Close()
		return nil, fmt.Errorf("failed to set permissions on socket file: %w", err)
	}

	logging.Info("Server listening on Unix socket: %s", socketPath)

	// Start server with Unix socket
	go func() {
		if err := server.Serve(unixListener); err != nil && err != http.ErrServerClosed {
			logging.Error("Error starting server on Unix socket: %v", err)
			serverError <- err
		}
	}()

	return unixListener, nil
}

// StartServer starts the server and handles graceful shutdown
func (sm *ServerManager) StartServer() error {
	// Create channel for shutdown signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Start server in a goroutine
	serverError := make(chan error, 1)

	// Get server configuration
	_, ok := sm.Server.Handler.(*http.ServeMux)
	if !ok {
		// Try to get the config from the middleware chain
		if middleware, ok := sm.Server.Handler.(interface{ GetConfig() *config.Config }); ok {
			cfg := middleware.GetConfig()

			// Check if Unix socket is configured
			if cfg != nil && cfg.Server.UnixSocketPath != "" {
				_, err := setupUnixSocket(sm.Server, cfg.Server.UnixSocketPath, serverError)
				if err != nil {
					return fmt.Errorf("failed to setup Unix socket: %w", err)
				}

				// If TCP address is also configured, start it as well
				if cfg.Server.ListenAddress != "" {
					logging.Info("Server also listening on TCP: %s", sm.Server.Addr)
				}

				// Wait for shutdown signal or server error
				select {
				case <-stop:
					logging.Info("Shutting down server...")
				case err := <-serverError:
					return err
				}

				// Create shutdown context with timeout
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				// Shutdown server gracefully
				if err := sm.Server.Shutdown(ctx); err != nil {
					return fmt.Errorf("server shutdown failed: %w", err)
				}

				// Remove socket file
				if err := os.Remove(cfg.Server.UnixSocketPath); err != nil {
					logging.Warning("Failed to remove socket file: %v", err)
				}

				logging.Info("Server gracefully stopped")
				return nil
			}
		}
	}

	// Default TCP server startup
	go func() {
		logging.Info("Server listening on %s", sm.Server.Addr)
		if err := sm.Server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logging.Error("Error starting server: %v", err)
			serverError <- err
		}
	}()

	// Wait for shutdown signal or server error
	select {
	case <-stop:
		logging.Info("Shutting down server...")
	case err := <-serverError:
		return err
	}

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Shutdown server gracefully
	if err := sm.Server.Shutdown(ctx); err != nil {
		return fmt.Errorf("server shutdown failed: %w", err)
	}

	logging.Info("Server gracefully stopped")
	return nil
}

func main() {
	// Parse command line flags
	configPath := flag.String("config", "config.json", "Path to configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	// Configure logging
	if err := setupLogging(cfg); err != nil {
		log.Fatalf("Error setting up logging: %v", err)
	}

	// Initialize cache
	cacheInitializer := &CacheInitializer{Config: cfg}
	cache, headerCache, validationCache, err := cacheInitializer.Initialize()
	if err != nil {
		logging.Fatal("Failed to initialize cache: %v", err)
	}

	// Create HTTP client with appropriate timeout and proxy settings
	client := createHTTPClient(cfg)

	// Create server
	server := createServer(cfg, cache, headerCache, validationCache, client)

	// Start server in a goroutine
	go startServer(server, cfg)

	// Wait for shutdown signal
	waitForShutdown(server)
}

// setupLogging configures the logging system based on configuration
func setupLogging(cfg config.Config) error {
	// Configure logging based on the config
	logging.Info("Setting up logging with level: %s", cfg.Logging.Level)

	// Create logging config
	logConfig := logging.LogConfig{
		FilePath:        cfg.Logging.FilePath,
		DisableTerminal: cfg.Logging.DisableTerminal,
		MaxSize:         cfg.Logging.MaxSize,
		Level:           logging.ParseLogLevel(cfg.Logging.Level),
	}

	// Initialize logger
	return logging.Initialize(logConfig)
}

// createHTTPClient creates an HTTP client with appropriate timeout and proxy settings
func createHTTPClient(cfg config.Config) *http.Client {
	// Get timeout from config or use default
	timeoutSeconds := cfg.Server.Timeout
	if timeoutSeconds <= 0 {
		timeoutSeconds = 30 // Default timeout
	}

	// Use the utility function which handles environment proxy configuration
	return utils.CreateHTTPClient(timeoutSeconds)
}

// createServer creates and configures the HTTP server
func createServer(cfg config.Config, cache storage.Cache, headerCache storage.HeaderCache, validationCache storage.ValidationCache, client *http.Client) *http.Server {
	// Create router
	mux := http.NewServeMux()

	// Register repository handlers
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})

	// Register repository handlers
	for _, repo := range cfg.Repositories {
		if !repo.Enabled {
			logging.Info("Skipping disabled repository: %s", repo.URL)
			continue
		}

		// Normalize paths
		basePath := repo.Path
		if !strings.HasPrefix(basePath, "/") {
			basePath = "/" + basePath
		}
		if !strings.HasSuffix(basePath, "/") {
			basePath += "/"
		}

		upstreamURL := repo.URL
		if !strings.HasSuffix(upstreamURL, "/") {
			upstreamURL += "/"
		}

		logging.Info("Setting up mirror for %s at path %s", upstreamURL, basePath)

		// Create handler for this repository
		handler := handlers.NewRepositoryHandler(
			upstreamURL,
			cache,
			headerCache,
			validationCache,
			client,
			basePath,
		)

		// Register the handler
		mux.Handle(basePath, handler)
	}

	// Create server
	server := &http.Server{
		Addr:         cfg.Server.ListenAddress,
		Handler:      mux,
		ReadTimeout:  time.Duration(cfg.Server.Timeout) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.Timeout) * time.Second,
		IdleTimeout:  time.Duration(cfg.Server.Timeout*2) * time.Second,
	}

	// Unix socket configuration is now handled in startServer
	// We don't need to configure it here since it would create a race condition
	// with multiple listeners

	return server
}

// startServer starts the server and handles errors
func startServer(server *http.Server, cfg config.Config) {
	// Create error channel
	serverError := make(chan error, 1)

	// Start server in a goroutine
	go func() {
		// Check if Unix socket is configured
		if cfg.Server.UnixSocketPath != "" {
			_, err := setupUnixSocket(server, cfg.Server.UnixSocketPath, serverError)
			if err != nil {
				logging.Error("Failed to setup Unix socket: %v", err)
				serverError <- err
				return
			}

			// If TCP address is also configured, start it as well
			if cfg.Server.ListenAddress != "" {
				logging.Info("Server also listening on TCP: %s", server.Addr)
				if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					logging.Error("Error starting server on TCP: %v", err)
					serverError <- err
				}
			}
		} else {
			// Only TCP
			logging.Info("Server listening on: %s", server.Addr)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logging.Error("Error starting server: %v", err)
				serverError <- err
			}
		}
	}()

	// Wait for server error or successful start
	select {
	case err := <-serverError:
		logging.Fatal("Server error: %v", err)
	case <-time.After(500 * time.Millisecond):
		// Server started successfully
		logging.Info("Server started successfully")
	}
}

// waitForShutdown waits for shutdown signal and gracefully shuts down the server
func waitForShutdown(server *http.Server) {
	// Create channel for shutdown signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Wait for shutdown signal
	<-stop
	logging.Info("Shutting down server...")

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Shutdown server gracefully
	if err := server.Shutdown(ctx); err != nil {
		logging.Error("Server shutdown failed: %v", err)
	}

	// Check if we need to remove a Unix socket file
	if middleware, ok := server.Handler.(interface{ GetConfig() *config.Config }); ok {
		if cfg := middleware.GetConfig(); cfg != nil && cfg.Server.UnixSocketPath != "" {
			// Remove socket file
			if err := os.Remove(cfg.Server.UnixSocketPath); err != nil {
				logging.Warning("Failed to remove socket file: %v", err)
			}
		}
	}

	logging.Info("Server gracefully stopped")
}
