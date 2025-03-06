package main

import (
	"context"
	"flag"
	"fmt"
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

type CacheInitializer struct {
	Config config.Config
}

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

	if !filepath.IsAbs(cacheDir) {
		absPath, err := filepath.Abs(cacheDir)
		if err == nil {
			cacheDir = absPath
		}
	}

	if err := utils.CreateDirectory(cacheDir); err != nil {
		return nil, nil, nil, utils.WrapError("failed to create cache directory", err)
	}

	logging.Info("Creating cache directory at %s", cacheDir)

	var cache storage.Cache
	var headerCache storage.HeaderCache
	var err error

	if cfg.Cache.LRU {
		maxSizeBytes, err := utils.ParseSize(cfg.Cache.MaxSize)
		if err != nil {
			maxSizeBytes = 1024 * 1024 * 1024
			logging.Warning("Invalid cache max size '%s', defaulting to 1GB", cfg.Cache.MaxSize)
		}

		if cfg.Cache.CleanOnStart {
			if err := os.RemoveAll(cacheDir); err != nil {
				return nil, nil, nil, utils.WrapError("failed to clean cache directory", err)
			}
			if err := os.MkdirAll(cacheDir, 0755); err != nil {
				return nil, nil, nil, utils.WrapError("failed to recreate cache directory", err)
			}
		}

		lruOptions := storage.LRUCacheOptions{
			BasePath:     cacheDir,
			MaxSizeBytes: maxSizeBytes,
			CleanOnStart: cfg.Cache.CleanOnStart,
		}
		lruCache, err := storage.NewLRUCacheWithOptions(lruOptions)
		if err != nil {
			return nil, nil, nil, utils.WrapError("failed to create LRU cache", err)
		}

		itemCount, currentSize, maxSize := lruCache.GetCacheStats()
		logging.Info("LRU cache initialized with %d items, current size: %d bytes, max size: %d bytes",
			itemCount, currentSize, maxSize)
		logging.Info("Using LRU disk cache at %s (max size: %s)", cacheDir, cfg.Cache.MaxSize)

		cache = lruCache
	} else {
		cache = storage.NewNoopCache()
	}

	headerCache, err = storage.NewFileHeaderCache(cacheDir)
	if err != nil {
		return nil, nil, nil, utils.WrapError("failed to create header cache", err)
	}
	logging.Info("Using header cache at %s", cacheDir)

	validationTTL := time.Duration(cfg.Cache.ValidationCacheTTL) * time.Second
	validationCache := storage.NewMemoryValidationCache(validationTTL)
	logging.Info("Using in-memory validation cache with TTL of %v", validationTTL)

	return cache, headerCache, validationCache, nil
}

type ServerSetup struct {
	Config          *config.Config
	Cache           storage.Cache
	HeaderCache     storage.HeaderCache
	ValidationCache storage.ValidationCache
	HTTPClient      *http.Client
}

func (ss *ServerSetup) CreateServer() *http.Server {
	mux := http.NewServeMux()

	ss.registerRepositoryHandlers(mux)

	mux.HandleFunc("/status", ss.handleStatus)

	middlewareChain := handlers.CreateMiddlewareChain(ss.Config)
	handler := middlewareChain.Apply(mux)

	server := &http.Server{
		Addr:         ss.Config.Server.ListenAddress,
		Handler:      handler,
		ReadTimeout:  time.Duration(ss.Config.Server.Timeout) * time.Second,
		WriteTimeout: time.Duration(ss.Config.Server.Timeout) * time.Second,
		IdleTimeout:  time.Duration(ss.Config.Server.Timeout*2) * time.Second,
	}

	return server
}

func (ss *ServerSetup) registerRepositoryHandlers(mux *http.ServeMux) {
	for _, repo := range ss.Config.Repositories {
		if !repo.Enabled {
			logging.Info("Skipping disabled repository: %s", repo.URL)
			continue
		}

		basePath := utils.NormalizeBasePath(repo.Path)
		upstreamURL := utils.NormalizeURL(repo.URL) + "/"

		logging.Info("Setting up mirror for %s at path %s", upstreamURL, basePath)

		handler := handlers.NewRepositoryHandler(
			upstreamURL,
			ss.Cache,
			ss.HeaderCache,
			ss.ValidationCache,
			ss.HTTPClient,
			basePath,
		)

		mux.Handle(basePath, http.StripPrefix(basePath, handler))
	}
}

func (ss *ServerSetup) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
}

type ConfigManager struct {
	ConfigFile       string
	CreateConfigFlag bool
	CommandLineFlags map[string]interface{}
}

func NewConfigManager() *ConfigManager {
	cm := &ConfigManager{
		CommandLineFlags: make(map[string]interface{}),
	}

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

	flag.Parse()

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

func (cm *ConfigManager) LoadConfig() (config.Config, error) {
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

	cfg, err := config.LoadConfig(cm.ConfigFile)
	if err != nil {
		logging.Warning("Error loading config: %v", err)
		logging.Info("Using default configuration")
		cfg = config.DefaultConfig()
	}

	cm.applyCommandLineFlags(&cfg)

	if err := config.ValidateConfig(cfg); err != nil {
		return cfg, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

func (cm *ConfigManager) applyCommandLineFlags(cfg *config.Config) {
	if listenAddr, ok := cm.CommandLineFlags["listenAddr"].(string); ok && listenAddr != "" {
		cfg.Server.ListenAddress = listenAddr
	}

	if unixSocketPath, ok := cm.CommandLineFlags["unixSocketPath"].(string); ok && unixSocketPath != "" {
		cfg.Server.UnixSocketPath = unixSocketPath
	}

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

type ServerManager struct {
	Server *http.Server
}

func setupUnixSocket(server *http.Server, socketPath string, serverError chan<- error) (net.Listener, error) {
	if _, err := os.Stat(socketPath); err == nil {
		if err := os.Remove(socketPath); err != nil {
			return nil, fmt.Errorf("failed to remove existing socket file: %w", err)
		}
	}

	unixListener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create Unix socket listener: %w", err)
	}

	if err := os.Chmod(socketPath, 0666); err != nil {
		unixListener.Close()
		return nil, fmt.Errorf("failed to set permissions on socket file: %w", err)
	}

	logging.Info("Server listening on Unix socket: %s", socketPath)

	go func() {
		if err := server.Serve(unixListener); err != nil && err != http.ErrServerClosed {
			logging.Error("Error starting server on Unix socket: %v", err)
			serverError <- err
		}
	}()

	return unixListener, nil
}

func (sm *ServerManager) StartServer() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	serverError := make(chan error, 1)

	_, ok := sm.Server.Handler.(*http.ServeMux)
	if !ok {
		if middleware, ok := sm.Server.Handler.(interface{ GetConfig() *config.Config }); ok {
			cfg := middleware.GetConfig()

			if cfg != nil && cfg.Server.UnixSocketPath != "" {
				_, err := setupUnixSocket(sm.Server, cfg.Server.UnixSocketPath, serverError)
				if err != nil {
					return fmt.Errorf("failed to setup Unix socket: %w", err)
				}

				if cfg.Server.ListenAddress != "" {
					logging.Info("Server also listening on TCP: %s", sm.Server.Addr)
				}

				select {
				case <-stop:
					logging.Info("Shutting down server...")
				case err := <-serverError:
					return err
				}

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				if err := sm.Server.Shutdown(ctx); err != nil {
					return fmt.Errorf("server shutdown failed: %w", err)
				}

				if err := os.Remove(cfg.Server.UnixSocketPath); err != nil {
					logging.Warning("Failed to remove socket file: %v", err)
				}

				logging.Info("Server gracefully stopped")
				return nil
			}
		}
	}

	go func() {
		logging.Info("Server listening on %s", sm.Server.Addr)
		if err := sm.Server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logging.Error("Error starting server: %v", err)
			serverError <- err
		}
	}()

	select {
	case <-stop:
		logging.Info("Shutting down server...")
	case err := <-serverError:
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := sm.Server.Shutdown(ctx); err != nil {
		return fmt.Errorf("server shutdown failed: %w", err)
	}

	logging.Info("Server gracefully stopped")
	return nil
}

func main() {
	configPath := flag.String("config", "config.json", "Path to configuration file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logging.Fatal("Error loading configuration: %v", err)
	}

	if err := setupLogging(cfg); err != nil {
		logging.Fatal("Error setting up logging: %v", err)
	}

	cacheInitializer := &CacheInitializer{Config: cfg}
	cache, headerCache, validationCache, err := cacheInitializer.Initialize()
	if err != nil {
		logging.Fatal("Failed to initialize cache: %v", err)
	}

	client := createHTTPClient(cfg)

	server := createServer(cfg, cache, headerCache, validationCache, client)

	go startServer(server, cfg)

	waitForShutdown(server)
}

func setupLogging(cfg config.Config) error {
	logging.Info("Setting up logging with level: %s", cfg.Logging.Level)

	logConfig := logging.LogConfig{
		FilePath:        cfg.Logging.FilePath,
		DisableTerminal: cfg.Logging.DisableTerminal,
		MaxSize:         cfg.Logging.MaxSize,
		Level:           logging.ParseLogLevel(cfg.Logging.Level),
	}

	return logging.Initialize(logConfig)
}

func createHTTPClient(cfg config.Config) *http.Client {
	timeoutSeconds := cfg.Server.Timeout
	if timeoutSeconds <= 0 {
		timeoutSeconds = 30
	}

	return utils.CreateHTTPClient(timeoutSeconds)
}

func createServer(cfg config.Config, cache storage.Cache, headerCache storage.HeaderCache, validationCache storage.ValidationCache, client *http.Client) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})

	for _, repo := range cfg.Repositories {
		if !repo.Enabled {
			logging.Info("Skipping disabled repository: %s", repo.URL)
			continue
		}

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

		handler := handlers.NewRepositoryHandler(
			upstreamURL,
			cache,
			headerCache,
			validationCache,
			client,
			basePath,
		)

		mux.Handle(basePath, handler)
	}
	server := &http.Server{
		Addr:         cfg.Server.ListenAddress,
		Handler:      mux, // Используем ServeMux напрямую
		ReadTimeout:  time.Duration(cfg.Server.Timeout) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.Timeout) * time.Second,
		IdleTimeout:  time.Duration(cfg.Server.Timeout*2) * time.Second,
	}

	return server
}
func startServer(server *http.Server, cfg config.Config) {
	serverError := make(chan error, 1)
	go func() {
		if cfg.Server.UnixSocketPath != "" {
			_, err := setupUnixSocket(server, cfg.Server.UnixSocketPath, serverError)
			if err != nil {
				logging.Error("Failed to setup Unix socket: %v", err)
				serverError <- err
				return
			}

			if cfg.Server.ListenAddress != "" {
				logging.Info("Server also listening on TCP: %s", server.Addr)
				if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					logging.Error("Error starting server on TCP: %v", err)
					serverError <- err
				}
			}
		} else {
			logging.Info("Server listening on: %s", server.Addr)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logging.Error("Error starting server: %v", err)
				serverError <- err
			}
		}
	}()
	select {
	case err := <-serverError:
		logging.Fatal("Server error: %v", err)
	case <-time.After(500 * time.Millisecond):
		logging.Info("Server started successfully")
	}
}

func waitForShutdown(server *http.Server) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	logging.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logging.Error("Server shutdown failed: %v", err)
	}

	if middleware, ok := server.Handler.(interface{ GetConfig() *config.Config }); ok {
		if cfg := middleware.GetConfig(); cfg != nil && cfg.Server.UnixSocketPath != "" {
			if err := os.Remove(cfg.Server.UnixSocketPath); err != nil {
				logging.Warning("Failed to remove socket file: %v", err)
			}
		}
	}

	logging.Info("Server gracefully stopped")
}
