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
	"path/filepath"
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

	if err := run(ctx, os.Args, stop); err != nil {

		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)

		_ = logging.Sync()
		os.Exit(1)
	}
	fmt.Println("Server shut down gracefully.")

	_ = logging.Sync()
}

func run(ctx context.Context, args []string, stop context.CancelFunc) error {

	flags := flag.NewFlagSet(args[0], flag.ExitOnError)
	configFile := flags.String("config", "config.json", "Path to configuration file")
	createConfig := flags.Bool("create-config", false, "Create default configuration file if it doesn't exist and exit")

	listenAddr := flags.String("listen", "", "Override server.listenAddress (e.g., :8080)")
	unixSocket := flags.String("unix-socket", "", "Override server.unixSocketPath")
	cacheDir := flags.String("cache-dir", "", "Override cache.directory")
	cacheSize := flags.String("cache-size", "", "Override cache.maxSize (e.g., 1GB)")
	logLevel := flags.String("log-level", "", "Override logging.level (debug, info, warn, error)")

	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage of %s:\n", args[0])
		flags.PrintDefaults()
		fmt.Fprintf(flags.Output(), "\nExample: %s -config /etc/go-apt-proxy/config.json -log-level debug\n", args[0])
	}

	if err := flags.Parse(args[1:]); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	if *createConfig {
		if err := config.EnsureDefaultConfig(*configFile); err != nil {

			fmt.Fprintf(os.Stderr, "Failed to ensure default config at %s: %v\n", *configFile, err)
			return fmt.Errorf("failed to ensure default config: %w", err)
		}
		fmt.Printf("Default config ensured at %s. Please review and adjust.\n", *configFile)
		return nil
	}

	cfg, err := config.Load(*configFile)
	if err != nil {

		fmt.Fprintf(os.Stderr, "Failed to load configuration from %s: %v\n", *configFile, err)
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

		fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
		return fmt.Errorf("invalid configuration: %w", err)
	}

	if err := logging.Setup(cfg.Logging); err != nil {

		fmt.Fprintf(os.Stderr, "Failed to setup logging: %v\n", err)
		return fmt.Errorf("failed to setup logging: %w", err)
	}

	defer logging.Sync()

	logging.Info("Configuration loaded and validated successfully", "config_file", *configFile)

	logging.Info("Initializing cache...")
	cacheManager, err := cache.NewDiskLRUCache(cfg.Cache)
	if err != nil {
		logging.ErrorE("Failed to initialize cache", err)
		return fmt.Errorf("failed to initialize cache: %w", err)
	}
	defer cacheManager.Close()
	logging.Info("Cache initialized.")
	stats := cacheManager.Stats()
	logging.Info("Initial cache stats",
		"enabled", stats.CacheEnabled,
		"directory", stats.CacheDirectory,
		"items", stats.ItemCount,
		"current_size", util.FormatSize(stats.CurrentSize),
		"max_size", util.FormatSize(stats.MaxSize),
		"current_size_bytes", stats.CurrentSize,
		"max_size_bytes", stats.MaxSize,
	)

	fetcher := fetch.NewCoordinator(cfg.Server.RequestTimeout.Duration(), cfg.Server.MaxConcurrentFetches)
	logging.Info("Fetch coordinator initialized", "max_concurrent", cfg.Server.MaxConcurrentFetches, "timeout", cfg.Server.RequestTimeout.Duration())

	srv, err := server.New(cfg, cacheManager, fetcher)
	if err != nil {
		logging.ErrorE("Failed to create server", err)
		return fmt.Errorf("failed to create server: %w", err)
	}

	listeners := make([]net.Listener, 0, 2)

	if cfg.Server.ListenAddress != "" {
		tcpListener, err := net.Listen("tcp", cfg.Server.ListenAddress)
		if err != nil {
			logging.ErrorE("Failed to listen on TCP", err, "address", cfg.Server.ListenAddress)
			return fmt.Errorf("failed to listen on TCP %s: %w", cfg.Server.ListenAddress, err)
		}
		listeners = append(listeners, tcpListener)
		logging.Info("Listening on TCP", "address", tcpListener.Addr().String())
	}

	if cfg.Server.UnixSocketPath != "" {

		socketPath := util.CleanPath(cfg.Server.UnixSocketPath)

		socketDir := filepath.Dir(socketPath)
		if err := os.MkdirAll(socketDir, 0755); err != nil {
			logging.ErrorE("Failed to create directory for unix socket", err, "directory", socketDir)

			for _, l := range listeners {
				_ = l.Close()
			}
			return fmt.Errorf("failed to create directory for unix socket %s: %w", socketDir, err)
		}

		if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			logging.Warn("Failed to remove existing unix socket file, listen may fail", "error", err, "path", socketPath)
		}

		unixListener, err := net.Listen("unix", socketPath)
		if err != nil {
			logging.ErrorE("Failed to listen on Unix socket", err, "path", socketPath)
			for _, l := range listeners {
				_ = l.Close()
			}
			return fmt.Errorf("failed to listen on Unix socket %s: %w", socketPath, err)
		}

		perms := cfg.Server.UnixSocketPermissions.FileMode()
		if err := os.Chmod(socketPath, perms); err != nil {
			unixListener.Close()
			os.Remove(socketPath)
			for _, l := range listeners {
				_ = l.Close()
			}
			logging.ErrorE("Failed to set permissions on unix socket", err, "path", socketPath, "permissions", fmt.Sprintf("%o", perms))
			return fmt.Errorf("failed to set permissions %o on socket %s: %w", perms, socketPath, err)
		}

		listeners = append(listeners, unixListener)
		logging.Info("Listening on Unix socket", "path", socketPath, "permissions", fmt.Sprintf("%o", perms))

		defer func() {
			logging.Debug("Removing unix socket file", "path", socketPath)
			if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				logging.Warn("Failed to remove unix socket file during shutdown", "error", err, "path", socketPath)
			}
		}()
	}

	if len(listeners) == 0 {
		return errors.New("no listeners configured (server.listenAddress or server.unixSocketPath must be set)")
	}

	errChan := make(chan error, len(listeners))
	for _, l := range listeners {
		listener := l
		go func() {
			listenerAddr := listener.Addr().String()
			logging.Info("Starting server on listener", "address", listenerAddr)

			if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {

				logging.ErrorE("Server error on listener", serveErr, "address", listenerAddr)
				errChan <- fmt.Errorf("server error on %s: %w", listenerAddr, serveErr)
			} else {

				logging.Info("Server stopped accepting connections on listener", "address", listenerAddr)
			}
		}()
	}

	select {
	case err := <-errChan:

		logging.Error("Listener failed, initiating shutdown...", "error", err)

		stop()

		shutdownCtxErr, cancelShutdownErr := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout.Duration())
		defer cancelShutdownErr()
		if shutdownErr := srv.Shutdown(shutdownCtxErr); shutdownErr != nil {
			logging.ErrorE("Shutdown failed after listener error", shutdownErr)
		}
		return err

	case <-ctx.Done():

		logging.Info("Shutdown signal received, initiating graceful shutdown...")

	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout.Duration())
	defer cancelShutdown()

	logging.Info("Attempting graceful server shutdown...", "timeout", cfg.Server.ShutdownTimeout.Duration())
	shutdownErr := srv.Shutdown(shutdownCtx)
	if shutdownErr != nil {

		logging.ErrorE("Graceful shutdown failed", shutdownErr)

		_ = srv.Close()
		return fmt.Errorf("graceful shutdown failed: %w", shutdownErr)
	}

	logging.Info("Server shutdown complete.")
	return nil
}
