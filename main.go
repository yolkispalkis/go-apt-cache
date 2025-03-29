package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

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

	if err := run(ctx, os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Server shut down gracefully.")
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet(args[0], flag.ExitOnError)
	configFile := flags.String("config", "config.json", "Path to configuration file")
	createConfig := flags.Bool("create-config", false, "Create default configuration file if it doesn't exist")
	listenAddr := flags.String("listen", "", "Address to listen on (e.g., :8080)")
	unixSocket := flags.String("unix-socket", "", "Path to Unix domain socket")
	cacheDir := flags.String("cache-dir", "", "Cache directory")
	cacheSize := flags.String("cache-size", "", "Maximum cache size (e.g., 1GB)")
	logLevel := flags.String("log-level", "", "Log level (debug, info, warn, error)")

	if err := flags.Parse(args[1:]); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	if *createConfig {
		if err := config.EnsureDefaultConfig(*configFile); err != nil {
			return fmt.Errorf("failed to ensure default config: %w", err)
		}
		fmt.Printf("Default config ensured at %s. Please review and adjust.\n", *configFile)
		return nil
	}

	cfg, err := config.Load(*configFile)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	if *listenAddr != "" {
		cfg.Server.ListenAddress = *listenAddr
	}
	if *unixSocket != "" {
		cfg.Server.UnixSocketPath = *unixSocket
	}
	if *cacheDir != "" {
		cfg.Cache.Directory = *cacheDir
	}
	if *cacheSize != "" {
		cfg.Cache.MaxSize = *cacheSize
	}
	if *logLevel != "" {
		cfg.Logging.Level = *logLevel
	}

	if err := config.Validate(cfg); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	if err := logging.Setup(cfg.Logging); err != nil {
		return fmt.Errorf("failed to setup logging: %w", err)
	}
	defer logging.Sync()

	logging.Info("Configuration loaded successfully.")

	logging.Info("Initializing cache...")
	cacheManager, err := cache.NewDiskLRUCache(cfg.Cache)
	if err != nil {
		return fmt.Errorf("failed to initialize cache: %w", err)
	}
	defer cacheManager.Close()
	logging.Info("Cache initialized.")
	stats := cacheManager.Stats()
	logging.Info("Initial cache stats: Items=%d, Size=%s, MaxSize=%s",
		stats.ItemCount, util.FormatSize(stats.CurrentSize), util.FormatSize(stats.MaxSize))

	fetcher := fetch.NewCoordinator(cfg.Server.RequestTimeout.Duration(), cfg.Server.MaxConcurrentFetches)
	logging.Info("Fetch coordinator initialized.")

	srv, err := server.New(cfg, cacheManager, fetcher)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	listeners := make([]net.Listener, 0, 2)
	if cfg.Server.ListenAddress != "" {
		tcpListener, err := net.Listen("tcp", cfg.Server.ListenAddress)
		if err != nil {
			return fmt.Errorf("failed to listen on TCP %s: %w", cfg.Server.ListenAddress, err)
		}
		listeners = append(listeners, tcpListener)
		logging.Info("Listening on TCP: %s", cfg.Server.ListenAddress)
	}

	if cfg.Server.UnixSocketPath != "" {
		socketDir := util.CleanPathDir(cfg.Server.UnixSocketPath)
		if err := os.MkdirAll(socketDir, 0755); err != nil {
			return fmt.Errorf("failed to create directory for unix socket %s: %w", socketDir, err)
		}
		_ = os.Remove(cfg.Server.UnixSocketPath)

		unixListener, err := net.Listen("unix", cfg.Server.UnixSocketPath)
		if err != nil {
			return fmt.Errorf("failed to listen on Unix socket %s: %w", cfg.Server.UnixSocketPath, err)
		}
		if err := os.Chmod(cfg.Server.UnixSocketPath, cfg.Server.UnixSocketPermissions.FileMode()); err != nil {
			unixListener.Close()
			os.Remove(cfg.Server.UnixSocketPath)
			return fmt.Errorf("failed to set permissions %o on socket %s: %w", cfg.Server.UnixSocketPermissions, cfg.Server.UnixSocketPath, err)
		}

		listeners = append(listeners, unixListener)
		logging.Info("Listening on Unix socket: %s (Permissions: %o)", cfg.Server.UnixSocketPath, cfg.Server.UnixSocketPermissions)

		defer func() {
			logging.Debug("Removing unix socket: %s", cfg.Server.UnixSocketPath)
			if err := os.Remove(cfg.Server.UnixSocketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				logging.Warn("Failed to remove unix socket %s: %v", cfg.Server.UnixSocketPath, err)
			}
		}()
	}

	if len(listeners) == 0 {
		return errors.New("no listeners configured (TCP or Unix socket)")
	}

	errChan := make(chan error, len(listeners))
	for _, l := range listeners {
		listener := l
		go func() {
			logging.Info("Starting server on listener: %s", listener.Addr())
			if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logging.Error("Server error on listener %s: %v", listener.Addr(), err)
				errChan <- fmt.Errorf("server error on %s: %w", listener.Addr(), err)
			} else {
				logging.Info("Server stopped accepting connections on listener: %s", listener.Addr())
			}
		}()
	}

	select {
	case err := <-errChan:
		logging.Error("Listener failed: %v", err)
	case <-ctx.Done():
		logging.Info("Shutdown signal received...")
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout.Duration())
	defer cancelShutdown()

	logging.Info("Attempting graceful server shutdown (timeout: %s)...", cfg.Server.ShutdownTimeout.Duration())
	shutdownErr := srv.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		logging.Error("Graceful shutdown failed: %v", shutdownErr)
		_ = srv.Close()
		return fmt.Errorf("graceful shutdown failed: %w", shutdownErr)
	}

	logging.Info("Server shutdown complete.")
	return nil
}
