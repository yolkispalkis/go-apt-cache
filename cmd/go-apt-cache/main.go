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
	"sync"
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

	cacheDir, err := filepath.Abs(cfg.Cache.Directory)
	if err != nil {
		logging.Error("Failed to determine absolute path for cache directory: %v", err)
		cacheDir = "./cache" // Fallback to default
	}

	logging.Info("Creating cache directory at %s", cacheDir)

	if err := utils.CreateDirectory(cacheDir); err != nil {
		return nil, nil, nil, utils.WrapError("failed to create cache directory", err)
	}

	var cache storage.Cache
	var headerCache storage.HeaderCache

	if cfg.Cache.LRU {
		maxSizeBytes, err := utils.ParseSize(cfg.Cache.MaxSize)
		if err != nil {
			maxSizeBytes = config.DefaultCacheMaxSize
			logging.Warning("Invalid cache max size '%s' in config, defaulting to %s", cfg.Cache.MaxSize, utils.FormatSize(config.DefaultCacheMaxSize))
		}

		if cfg.Cache.CleanOnStart {
			if err := storage.CleanCacheDirectory(cacheDir); err != nil {
				return nil, nil, nil, utils.WrapError("failed to clean cache directory", err)
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
		logging.Info("LRU cache initialized with %d items, current size: %s, max size: %s",
			itemCount, utils.FormatSize(currentSize), utils.FormatSize(maxSize))
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
		ReadTimeout:  time.Duration(ss.Config.Server.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(ss.Config.Server.WriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(ss.Config.Server.IdleTimeout) * time.Second,
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
	var cfg config.Config
	var err error

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

	cfg, err = config.LoadConfig(cm.ConfigFile)
	if err != nil {
		logging.Warning("Error loading config: %v", err)
		logging.Info("Using default configuration")
		cfg = config.DefaultConfig()
		return cfg, fmt.Errorf("error loading config: %w", err)
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

	permissions := server.Handler.(interface{ GetConfig() *config.Config }).GetConfig().Server.UnixSocketPermissions
	if permissions == 0 {
		permissions = 0666
	}

	if err := os.Chmod(socketPath, permissions); err != nil {
		unixListener.Close()
		return nil, fmt.Errorf("failed to set permissions on socket file: %w", err)
	}

	logging.Info("Server listening on Unix socket: %s", socketPath)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.Serve(unixListener); err != nil && err != http.ErrServerClosed {
			logging.Error("Error starting server on Unix socket: %v", err)
			serverError <- err
		}
	}()

	go func() {
		wg.Wait()
		close(serverError)
	}()

	return unixListener, nil
}

func (sm *ServerManager) StartAndWait() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	serverError := make(chan error, 1)

	var unixListener net.Listener
	var err error

	if middleware, ok := sm.Server.Handler.(interface{ GetConfig() *config.Config }); ok {
		if cfg := middleware.GetConfig(); cfg != nil && cfg.Server.UnixSocketPath != "" {
			unixListener, err = setupUnixSocket(sm.Server, cfg.Server.UnixSocketPath, serverError)
			if err != nil {
				return fmt.Errorf("failed to setup Unix socket: %w", err)
			}

			if cfg.Server.ListenAddress != "" {
				logging.Info("Server also listening on TCP: %s", sm.Server.Addr)
			}
		}
	}

	go func() {
		var err error
		if unixListener != nil {
			err = sm.Server.Serve(unixListener)
		} else {
			logging.Info("Server listening on %s", sm.Server.Addr)
			err = sm.Server.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			logging.Error("Server error: %v", err)
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

	if middleware, ok := sm.Server.Handler.(interface{ GetConfig() *config.Config }); ok {
		if cfg := middleware.GetConfig(); cfg != nil && cfg.Server.UnixSocketPath != "" {
			if err := os.Remove(cfg.Server.UnixSocketPath); err != nil {
				logging.Warning("Failed to remove socket file: %v", err)
			}
		}
	}

	logging.Info("Server gracefully stopped")
	return nil
}

func main() {
	configManager := NewConfigManager()
	cfg, err := configManager.LoadConfig()
	if err != nil {
		logging.Fatal("Error loading configuration: %v", err)
	}

	if err := setupLogging(cfg); err != nil {
		logging.Fatal("Error setting up logging: %v", err)
	}
	defer logging.Close()

	cacheInitializer := &CacheInitializer{Config: cfg}
	cache, headerCache, validationCache, err := cacheInitializer.Initialize()
	if err != nil {
		logging.Fatal("Failed to initialize cache: %v", err)
	}

	client := createHTTPClient(cfg)

	serverSetup := &ServerSetup{
		Config:          &cfg,
		Cache:           cache,
		HeaderCache:     headerCache,
		ValidationCache: validationCache,
		HTTPClient:      client,
	}

	server := serverSetup.CreateServer()

	serverManager := &ServerManager{Server: server}
	if err := serverManager.StartAndWait(); err != nil {
		logging.Fatal("Server failed: %v", err)
	}
}

func setupLogging(cfg config.Config) error {
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
