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

	"github.com/yolkispalkis/go-apt-cache/internal/config"
	"github.com/yolkispalkis/go-apt-cache/internal/log"
	"github.com/yolkispalkis/go-apt-cache/internal/util"
)

type Server struct {
	http *http.Server
	cfg  config.ServerConfig
	log  *log.Logger
	lns  []net.Listener
}

func New(cfg config.ServerConfig, lg *log.Logger, h http.Handler) *Server {
	return &Server{
		http: &http.Server{
			Handler:           h,
			ReadHeaderTimeout: cfg.ReadHeaderTimeout,
			IdleTimeout:       cfg.IdleTimeout,
			ReadTimeout:       cfg.ReqTimeout,
			WriteTimeout:      cfg.ReqTimeout,
		},
		cfg: cfg,
		log: lg.WithComponent("server"),
	}
}

func (s *Server) Start() error {
	if err := s.open(); err != nil {
		return err
	}
	if len(s.lns) == 0 {
		return errors.New("no listeners configured")
	}
	errCh := make(chan error, len(s.lns))
	for _, ln := range s.lns {
		l := ln
		s.log.Info().Str("network", l.Addr().Network()).Str("addr", l.Addr().String()).Msg("listening")
		go func() {
			if err := s.http.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}()
	}
	if err, ok := <-errCh; ok {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	err := s.http.Shutdown(ctx)
	s.cleanupUnix()
	return err
}

func (s *Server) ShutdownTimeout() time.Duration { return s.cfg.ShutdownTimeout }

func (s *Server) open() error {
	if addr := s.cfg.ListenAddr; addr != "" {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("tcp listen: %w", err)
		}
		s.lns = append(s.lns, ln)
	}
	if sp := s.cfg.UnixPath; sp != "" {
		clean := util.CleanPath(sp)
		_ = os.Remove(clean)
		if err := os.MkdirAll(filepath.Dir(clean), 0755); err != nil {
			return err
		}
		uln, err := net.Listen("unix", clean)
		if err != nil {
			return err
		}
		perms := s.cfg.UnixPerms
		if perms == 0 {
			perms = 0660
		}
		if err := os.Chmod(clean, perms); err != nil {
			_ = uln.Close()
			return err
		}
		s.lns = append(s.lns, uln)
	}
	return nil
}

func (s *Server) cleanupUnix() {
	if s.cfg.UnixPath == "" {
		return
	}
	clean := util.CleanPath(s.cfg.UnixPath)
	if err := os.Remove(clean); err != nil && !os.IsNotExist(err) {
		s.log.Warn().Err(err).Str("path", clean).Msg("remove unix socket failed")
	}
}
