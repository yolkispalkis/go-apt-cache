package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/fetch"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/server"
	"github.com/yolkispalkis/go-apt-cache/internal/util" // For FormatSize in logging example
)

func main() {
	// Setup context for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop() // Ensure stop is called to release resources if main exits early

	err := run(ctx)
	if err != nil {
		// Use stderr for final error messages as logger might not be fully flushed or available
		fmt.Fprintf(os.Stderr, "Application exited with error: %v\n", err)
		// Attempt to sync logs one last time
		if logSyncErr := logging.Sync(); logSyncErr != nil {
			fmt.Fprintf(os.Stderr, "Failed to sync logs: %v\n", logSyncErr)
		}
		os.Exit(1)
	}

	fmt.Println("Application shut down gracefully.")
	if logSyncErr := logging.Sync(); logSyncErr != nil {
		fmt.Fprintf(os.Stderr, "Failed to sync logs on graceful shutdown: %v\n", logSyncErr)
	}
}

func run(ctx context.Context) error {
	// --- Configuration Loading & Flag Parsing ---
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	configFile := fs.String("config", "config.json", "Path to configuration file")
	createConfig := fs.Bool("create-config", false, "Create default configuration file if it doesn't exist and exit")
	// Add overrides for common settings
	logLevelOverride := fs.String("log-level", "", "Override logging.level (debug, info, warn, error)")
	listenAddrOverride := fs.String("listen", "", "Override server.listenAddress (e.g., :8080)")

	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage of %s:\n", os.Args[0])
		fs.PrintDefaults()
		fmt.Fprintf(fs.Output(), "\nExample: %s -config /etc/go-apt-proxy/config.json -log-level debug\n", os.Args[0])
	}
	if err := fs.Parse(os.Args[1:]); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	if *createConfig {
		if err := config.EnsureDefaultConfig(*configFile); err != nil {
			return fmt.Errorf("failed to ensure default config at %s: %w", *configFile, err)
		}
		fmt.Printf("Default config ensured/created at %s. Please review and adjust if necessary.\n", *configFile)
		return nil // Exit after creating config
	}

	cfg, err := config.Load(*configFile)
	if err != nil {
		// Try to setup basic logging to stderr if config load fails, so this message is seen
		_ = logging.Setup(logging.DefaultConfig()) // Use defaults for this emergency log
		logging.Get().Fatal().Err(err).Str("config_file", *configFile).Msg("Failed to load configuration")
		return fmt.Errorf("loading configuration from %s: %w", *configFile, err) // Should not reach here due to Fatal
	}

	// Apply command-line overrides
	if *logLevelOverride != "" {
		cfg.Logging.Level = *logLevelOverride
	}
	if *listenAddrOverride != "" {
		cfg.Server.ListenAddress = *listenAddrOverride
	}

	if err := config.Validate(cfg); err != nil {
		_ = logging.Setup(logging.DefaultConfig())
		logging.Get().Fatal().Err(err).Msg("Invalid configuration")
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// --- Logging Setup ---
	if err := logging.Setup(cfg.Logging); err != nil {
		// If logging setup fails, print to stderr and exit
		fmt.Fprintf(os.Stderr, "Failed to setup logging: %v\n", err)
		return fmt.Errorf("setting up logging: %w", err)
	}
	logger := logging.Get() // Get the configured global logger
	logger.Info().Str("config_file", *configFile).Msg("Configuration loaded and validated.")

	// --- Cache Manager Initialization ---
	logger.Info().Msg("Initializing cache manager...")
	cacheManager, err := cache.NewDiskLRUCache(cfg.Cache, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to initialize cache manager")
		return fmt.Errorf("initializing cache manager: %w", err)
	}
	if err := cacheManager.Init(ctx); err != nil {
		// Non-fatal init error (e.g. scan failed but cache can operate in a degraded mode or clean state)
		// DiskLRUCache Init might set an internal error state.
		logger.Error().Err(err).Msg("Cache manager initialization encountered an error. Cache might be empty or inconsistent.")
		// Depending on severity, might choose to exit. For now, continue.
	} else {
		logger.Info().
			Str("dir", cfg.Cache.Directory).
			Str("max_size", cfg.Cache.MaxSize).
			Bool("enabled", cfg.Cache.Enabled).
			Int64("initial_items", cacheManager.GetItemCount()).
			Str("initial_size", util.FormatSize(cacheManager.GetCurrentSize())).
			Msg("Cache manager initialized.")
	}
	defer func() {
		logger.Info().Msg("Closing cache manager...")
		if err := cacheManager.Close(); err != nil {
			logger.Error().Err(err).Msg("Error closing cache manager")
		}
	}()

	// --- Fetch Coordinator Initialization ---
	logger.Info().Msg("Initializing fetch coordinator...")
	fetchCoordinator := fetch.NewCoordinator(cfg.Server, logger)
	logger.Info().
		Str("user_agent", cfg.Server.UserAgent).
		Int("max_concurrent_fetches", cfg.Server.MaxConcurrentFetches).
		Msg("Fetch coordinator initialized.")

	// --- HTTP Server Initialization ---
	logger.Info().Msg("Initializing HTTP server...")
	httpServer, err := server.New(cfg, cacheManager, fetchCoordinator, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create HTTP server")
		return fmt.Errorf("creating HTTP server: %w", err)
	}

	// --- Start Server and Wait for Shutdown ---
	serverErrChan := make(chan error, 1)
	go func() {
		serverErrChan <- httpServer.Start(ctx)
	}()

	select {
	case err := <-serverErrChan:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			logger.Error().Err(err).Msg("Server exited with error")
			// Attempt graceful shutdown even if Start errored out, to clean up listeners etc.
			if shutdownErr := httpServer.Shutdown(); shutdownErr != nil {
				logger.Error().Err(shutdownErr).Msg("Error during shutdown after server error.")
			}
			return err // Return the original server error
		}
		logger.Info().Msg("Server has shut down.")
	case <-ctx.Done(): // Triggered by signal.NotifyContext or if parent context cancels
		logger.Info().Msg("Shutdown signal received, initiating graceful shutdown of server...")
		if shutdownErr := httpServer.Shutdown(); shutdownErr != nil {
			logger.Error().Err(shutdownErr).Msg("Error during graceful server shutdown.")
			return shutdownErr // Return shutdown error
		}
		logger.Info().Msg("Server has been shut down gracefully due to signal.")
	}

	// Final check on cache size if enabled
	if cfg.Cache.Enabled {
		logger.Info().
			Int64("final_items", cacheManager.GetItemCount()).
			Str("final_size", util.FormatSize(cacheManager.GetCurrentSize())).
			Msg("Final cache status.")
	}

	return nil
}
