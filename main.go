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

	"github.com/rs/zerolog"
	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/fetch"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/server"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var mainLogger zerolog.Logger = logging.L()

	if err := run(ctx); err != nil {
		mainLogger.Error().Err(err).Msg("Application run failed")
		_ = logging.Sync()
		os.Exit(1)
	}
	mainLogger.Info().Msg("Shutdown complete.")
	_ = logging.Sync()
}

func run(ctx context.Context) error {
	cfgPath, createCfg, logLevelOverride, listenAddrOverride := parseFlags()

	var currentLogger zerolog.Logger = logging.L()

	if createCfg {
		if err := config.EnsureDefault(cfgPath); err != nil {
			currentLogger.Error().Err(err).Str("path", cfgPath).Msg("Failed to ensure default config")
			return fmt.Errorf("ensure default config at %s: %w", cfgPath, err)
		}
		currentLogger.Info().Str("path", cfgPath).Msg("Default config ensured/created. Review and adjust.")
		return nil
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		currentLogger.Error().Err(err).Str("file", cfgPath).Msg("Failed to load config")
		return fmt.Errorf("load config %s: %w", cfgPath, err)
	}

	applyOverrides(cfg, logLevelOverride, listenAddrOverride)

	if setupErr := logging.Setup(cfg.Logging); setupErr != nil {

		var setupLogger zerolog.Logger = logging.L()
		setupLogger.Warn().Err(setupErr).Msg("Failed to fully setup logging from config, using previous/best-effort logger settings.")
	}

	var log zerolog.Logger = logging.L()

	if err := config.Validate(cfg); err != nil {
		log.Error().Err(err).Msg("Invalid config")
		return fmt.Errorf("invalid config: %w", err)
	}

	log.Info().Str("file", cfgPath).Msg("Config loaded and validated")

	cm, diskLRUErr := cache.NewDiskLRU(cfg.Cache, log)
	if diskLRUErr != nil {
		log.Error().Err(diskLRUErr).Msg("Failed to init cache manager")
		return fmt.Errorf("init cache: %w", diskLRUErr)
	}

	if initErr := cm.Init(ctx); initErr != nil {
		log.Error().Err(initErr).Msg("Cache manager init failed. Application will continue, but cache might be inconsistent or empty.")
	} else {
		log.Info().Str("dir", cfg.Cache.Dir).Str("size", cfg.Cache.MaxSize).
			Int64("items", cm.ItemCount()).Str("used", util.FormatSize(cm.CurrentSize())).
			Msg("Cache manager initialized")
	}
	defer func() {

		var deferLogger zerolog.Logger = logging.L()
		if closeErr := cm.Close(); closeErr != nil {
			deferLogger.Warn().Err(closeErr).Msg("Error closing cache manager")
		}
	}()

	fc := fetch.NewCoordinator(cfg.Server, log)
	log.Info().Str("ua", cfg.Server.UserAgent).Int("max_fetches", cfg.Server.MaxConcurrent).Msg("Fetch coordinator initialized")

	srv, serverNewErr := server.New(cfg, cm, fc, log)
	if serverNewErr != nil {
		log.Error().Err(serverNewErr).Msg("Failed to create HTTP server")
		return fmt.Errorf("create server: %w", serverNewErr)
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.Start(ctx)
	}()

	var selectErr error
	select {
	case selectErr = <-errChan:
		if selectErr != nil && !errors.Is(selectErr, context.Canceled) && !errors.Is(selectErr, http.ErrServerClosed) {
			log.Error().Err(selectErr).Msg("Server error during start/run")
			shutdownErr := srv.Shutdown()
			if shutdownErr != nil {
				log.Error().Err(shutdownErr).Msg("Error during server shutdown after a start error")
			}
			return selectErr
		}
		log.Info().Msg("Server Start goroutine exited gracefully or with ignored error.")
	case <-ctx.Done():
		log.Info().Msg("Shutdown signal received, stopping server...")
		if shutdownErr := srv.Shutdown(); shutdownErr != nil {
			log.Error().Err(shutdownErr).Msg("Server shutdown error")
			return shutdownErr
		}
		log.Info().Msg("Server gracefully shut down.")
	}

	if cfg.Cache.Enabled {
		log.Info().Int64("items", cm.ItemCount()).Str("used", util.FormatSize(cm.CurrentSize())).Msg("Final cache status")
	}
	return selectErr
}

func parseFlags() (cfgPath string, createCfg bool, logLevel, listenAddr string) {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fs.PrintDefaults()
	}

	fs.StringVar(&cfgPath, "config", "config.json", "Path to config file")
	fs.BoolVar(&createCfg, "create-config", false, "Create default config file and exit")
	fs.StringVar(&logLevel, "log-level", "", "Override log level (debug, info, warn, error)")
	fs.StringVar(&listenAddr, "listen", "", "Override server listen address (e.g., :8080)")

	if err := fs.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing flags: %v\n", err)
		os.Exit(2)
	}
	return
}

func applyOverrides(cfg *config.Config, logLevel, listenAddr string) {
	if logLevel != "" {
		cfg.Logging.Level = logLevel
	}
	if listenAddr != "" {
		cfg.Server.ListenAddr = listenAddr
	}
}
