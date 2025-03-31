package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/fetch"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/server"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

const pprofServerAddr = "localhost:6060"

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
	enablePprof := flags.Bool("enable-pprof", false, "Enable the pprof debugging server on "+pprofServerAddr)

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
		fmt.Printf("Default config ensured at %s. Please review and adjust if necessary.\n", *configFile)
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

	var pprofServer *http.Server
	if *enablePprof {
		pprofServer = &http.Server{
			Addr:              pprofServerAddr,
			Handler:           http.DefaultServeMux,
			ReadHeaderTimeout: 3 * time.Second,
		}
		go func() {
			logging.Info("Starting pprof debug server", "address", pprofServerAddr)
			if err := pprofServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logging.ErrorE("pprof debug server failed", err, "address", pprofServerAddr)
			} else {
				logging.Info("pprof debug server stopped", "address", pprofServerAddr)
			}
		}()
	} else {
		logging.Info("pprof debug server is disabled.")
	}

	logging.Info("Initializing cache...")
	cacheManager, err := cache.NewDiskLRUCache(cfg.Cache)
	if err != nil {
		logging.ErrorE("Failed to initialize cache", err)
		return fmt.Errorf("failed to initialize cache: %w", err)
	}
	defer func() {
		logging.Info("Closing cache manager...")
		if err := cacheManager.Close(); err != nil {
			logging.ErrorE("Error closing cache manager", err)
		} else {
			logging.Info("Cache manager closed.")
		}
	}()

	stats := cacheManager.Stats()
	logging.Info("Initial cache stats",
		"enabled", stats.CacheEnabled,
		"directory", stats.CacheDirectory,
		"items", stats.ItemCount,
		"current_size", util.FormatSize(stats.CurrentSize),
		"max_size", util.FormatSize(stats.MaxSize),
		"init_time_ms", stats.InitTimeMs,
		"init_error", stats.InitError,
	)

	fetcher := fetch.NewCoordinator(cfg.Server.RequestTimeout.Duration(), cfg.Server.MaxConcurrentFetches)
	logging.Info("Fetch coordinator initialized", "max_concurrent", cfg.Server.MaxConcurrentFetches, "timeout", cfg.Server.RequestTimeout.Duration())

	srv, err := server.New(cfg, cacheManager, fetcher)
	if err != nil {
		logging.ErrorE("Failed to create server", err)
		return fmt.Errorf("failed to create server: %w", err)
	}

	listeners := make([]net.Listener, 0, 2)
	var listenErr error
	var wgListeners sync.WaitGroup

	if cfg.Server.ListenAddress != "" {
		wgListeners.Add(1)
		go func() {
			defer wgListeners.Done()
			tcpListener, err := net.Listen("tcp", cfg.Server.ListenAddress)
			if err != nil {
				logging.ErrorE("Failed to listen on TCP", err, "address", cfg.Server.ListenAddress)
				listenErr = errors.Join(listenErr, fmt.Errorf("tcp listen %s: %w", cfg.Server.ListenAddress, err))
				return
			}
			listeners = append(listeners, tcpListener)
			logging.Info("Listening on TCP", "address", tcpListener.Addr().String())
		}()
	}

	if cfg.Server.UnixSocketPath != "" {
		wgListeners.Add(1)
		go func() {
			defer wgListeners.Done()
			socketPath := util.CleanPath(cfg.Server.UnixSocketPath)
			socketDir := filepath.Dir(socketPath)
			if err := os.MkdirAll(socketDir, 0755); err != nil {
				logging.ErrorE("Failed to create directory for unix socket", err, "directory", socketDir)
				listenErr = errors.Join(listenErr, fmt.Errorf("mkdir unix socket dir %s: %w", socketDir, err))
				return
			}
			if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				logging.Warn("Failed to remove existing unix socket file, listen may fail", "error", err, "path", socketPath)
			}

			unixListener, err := net.Listen("unix", socketPath)
			if err != nil {
				logging.ErrorE("Failed to listen on Unix socket", err, "path", socketPath)
				listenErr = errors.Join(listenErr, fmt.Errorf("unix listen %s: %w", socketPath, err))
				return
			}

			perms := cfg.Server.UnixSocketPermissions.FileMode()
			if err := os.Chmod(socketPath, perms); err != nil {
				_ = unixListener.Close()
				_ = os.Remove(socketPath)
				logging.ErrorE("Failed to set permissions on unix socket", err, "path", socketPath, "permissions", fmt.Sprintf("0%o", perms))
				listenErr = errors.Join(listenErr, fmt.Errorf("chmod socket %s to 0%o: %w", socketPath, perms, err))
				return
			}

			listeners = append(listeners, unixListener)
			logging.Info("Listening on Unix socket", "path", socketPath, "permissions", fmt.Sprintf("0%o", perms))
		}()
	}

	wgListeners.Wait()

	if listenErr != nil {
		stop()
		for _, l := range listeners {
			_ = l.Close()
		}
		if cfg.Server.UnixSocketPath != "" {
			_ = os.Remove(util.CleanPath(cfg.Server.UnixSocketPath))
		}
		return fmt.Errorf("failed to start listeners: %w", listenErr)
	}

	if len(listeners) == 0 {
		return errors.New("no listeners configured or started (server.listenAddress or server.unixSocketPath must be set and valid)")
	}

	errChan := make(chan error, len(listeners))
	for _, l := range listeners {
		listener := l
		go func() {
			listenerAddr := listener.Addr().String()
			networkType := listener.Addr().Network()
			logging.Info("Starting main server loop", "network", networkType, "address", listenerAddr)
			if serveErr := srv.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
				logging.ErrorE("Server loop error", serveErr, "network", networkType, "address", listenerAddr)
				errChan <- fmt.Errorf("server error on %s (%s): %w", listenerAddr, networkType, serveErr)
			} else {
				logging.Info("Server loop stopped gracefully", "network", networkType, "address", listenerAddr)
			}
		}()
	}

	select {
	case err := <-errChan:
		logging.Error("Listener/Server failed, initiating shutdown...", "error", err)
		stop()
		listenErr = err

	case <-ctx.Done():
		logging.Info("Shutdown signal received, initiating graceful shutdown...")
		listenErr = ctx.Err()
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout.Duration())
	defer cancelShutdown()

	logging.Info("Attempting graceful main server shutdown...", "timeout", cfg.Server.ShutdownTimeout.Duration())
	shutdownErr := srv.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		logging.ErrorE("Graceful main server shutdown failed", shutdownErr)
	} else {
		logging.Info("Main server shutdown complete.")
	}

	if pprofServer != nil {
		logging.Info("Shutting down pprof server...")
		pprofShutdownCtx, pprofCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer pprofCancel()
		if pprofErr := pprofServer.Shutdown(pprofShutdownCtx); pprofErr != nil {
			logging.ErrorE("pprof server shutdown failed", pprofErr)
			shutdownErr = errors.Join(shutdownErr, pprofErr)
		} else {
			logging.Info("pprof server shutdown complete.")
		}
	}

	if cfg.Server.UnixSocketPath != "" {
		socketPath := util.CleanPath(cfg.Server.UnixSocketPath)
		logging.Debug("Attempting removal of unix socket file post-shutdown", "path", socketPath)
		if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			logging.Warn("Failed to remove unix socket file during shutdown cleanup", "error", err, "path", socketPath)
		}
	}

	finalErr := listenErr
	if shutdownErr != nil {
		finalErr = errors.Join(finalErr, fmt.Errorf("shutdown error: %w", shutdownErr))
	}

	if finalErr != nil && !errors.Is(listenErr, context.Canceled) {
		return finalErr
	}

	return nil
}
