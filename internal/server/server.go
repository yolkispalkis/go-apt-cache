package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/yolkispalkis/go-apt-cache/internal/cache"
	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/fetch"
	"github.com/yolkispalkis/go-apt-cache/internal/logging"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type Application struct {
	Config  *config.Config
	Logger  *logging.Logger
	Cache   cache.Manager
	Fetcher *fetch.Coordinator
}

func NewApplication(cfg *config.Config, logger *logging.Logger, cache cache.Manager, fetcher *fetch.Coordinator) *Application {
	return &Application{
		Config:  cfg,
		Logger:  logger,
		Cache:   cache,
		Fetcher: fetcher,
	}
}

type Server struct {
	httpSrv   *http.Server
	cfg       config.ServerConfig
	log       *logging.Logger
	listeners []net.Listener
}

func New(cfg config.ServerConfig, log *logging.Logger, handler http.Handler) *Server {
	return &Server{
		httpSrv: &http.Server{
			Handler:           handler,
			ReadHeaderTimeout: cfg.ReadHeaderTimeout,
			IdleTimeout:       cfg.IdleTimeout,
		},
		cfg: cfg,
		log: log.WithComponent("server"),
	}
}

func (s *Server) Start() error {
	if err := s.setupListeners(); err != nil {
		return fmt.Errorf("failed to setup listeners: %w", err)
	}
	if len(s.listeners) == 0 {
		return errors.New("no listeners configured (check listenAddress and unixSocketPath in config)")
	}

	errChan := make(chan error, 1)
	for _, l := range s.listeners {
		lis := l
		s.log.Info().Str("network", lis.Addr().Network()).Str("address", lis.Addr().String()).Msg("Server listening")
		go func() {
			if err := s.httpSrv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.log.Error().Err(err).Str("address", lis.Addr().String()).Msg("HTTP server error")
				select {
				case errChan <- err:
				default:
				}
			}
		}()
	}

	return <-errChan
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.log.Info().Msg("Attempting graceful server shutdown...")
	err := s.httpSrv.Shutdown(ctx)
	s.cleanupSocket()
	return err
}

func (s *Server) Logger() *logging.Logger {
	return s.log
}

func (s *Server) ShutdownTimeout() time.Duration {
	return s.cfg.ShutdownTimeout
}

func (s *Server) setupListeners() error {
	if addr := s.cfg.ListenAddr; addr != "" {
		tcpLn, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("failed to listen on TCP %s: %w", addr, err)
		}
		s.listeners = append(s.listeners, tcpLn)
	}

	if sockPath := s.cfg.UnixPath; sockPath != "" {
		unixLn, err := s.setupUnixSocket(sockPath)
		if err != nil {
			return fmt.Errorf("failed to listen on Unix socket %s: %w", sockPath, err)
		}
		s.listeners = append(s.listeners, unixLn)
	}
	return nil
}

func (s *Server) setupUnixSocket(sockPath string) (net.Listener, error) {
	cleanSockPath := util.CleanPath(sockPath)
	if err := os.Remove(cleanSockPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to remove existing socket file: %w", err)
	}

	sockDir := filepath.Dir(cleanSockPath)
	if err := os.MkdirAll(sockDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create socket directory %s: %w", sockDir, err)
	}

	unixLn, err := net.Listen("unix", cleanSockPath)
	if err != nil {
		return nil, err
	}

	perms := s.cfg.UnixPerms
	if perms == 0 {
		perms = 0660
	}
	if err := os.Chmod(cleanSockPath, perms); err != nil {
		unixLn.Close()
		return nil, fmt.Errorf("failed to chmod unix socket %s to %0o: %w", cleanSockPath, perms, err)
	}
	return unixLn, nil
}

func (s *Server) cleanupSocket() {
	if s.cfg.UnixPath != "" {
		cleanSockPath := util.CleanPath(s.cfg.UnixPath)
		if err := os.Remove(cleanSockPath); err != nil && !os.IsNotExist(err) {
			s.log.Warn().Err(err).Str("path", cleanSockPath).Msg("Failed to remove unix socket file during shutdown.")
		} else {
			s.log.Info().Str("path", cleanSockPath).Msg("Unix socket file removed.")
		}
	}
}
