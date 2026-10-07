package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"atlas/services/bnpb/client-api/internal/clientapi"
)

func run(log *slog.Logger) error {
	cfg, err := clientapi.ConfigFromEnv()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	// Reuse Auth and Aggregator connections under concurrent load.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 128
	transport.MaxConnsPerHost = cfg.RequestCapacity
	api := &clientapi.API{Config: cfg, Client: &http.Client{Transport: transport, Timeout: cfg.OutboundTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, Log: log}
	server := &http.Server{Addr: cfg.Address, Handler: api.Handler(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 2*cfg.OutboundTimeout + time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	log.Info("started", "service", "client-api", "address", cfg.Address)
	select {
	case <-ctx.Done():
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		cancel()
	}
	shutdown, finish := context.WithTimeout(context.Background(), 5*time.Second)
	defer finish()
	_ = server.Shutdown(shutdown)
	return err
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("stopped", "service", "client-api", "reason", err.Error())
		os.Exit(1)
	}
}
