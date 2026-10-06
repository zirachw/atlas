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

	"atlas/services/bmkg/internal/bmkg"
)

func run(log *slog.Logger) error {
	cfg, err := bmkg.ConfigFromEnv()
	if err != nil {
		return err
	}
	store, err := bmkg.LoadSeed()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go bmkg.RunGenerator(ctx, store, bmkg.NewGenerator(bmkg.Seed, time.Now()), cfg.EventInterval, log)

	api := &bmkg.API{Store: store, APIKey: cfg.APIKey, Delay: cfg.Delay, Log: log}
	server := &http.Server{Addr: cfg.Address, Handler: api.Handler(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	log.Info("started", "service", "bmkg", "address", cfg.Address, "delay_ms", cfg.Delay.Milliseconds(), "event_interval", cfg.EventInterval.String(), "seed_events", len(store.Events(nil)))
	select {
	case <-ctx.Done():
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	shutdown, finish := context.WithTimeout(context.Background(), 5*time.Second)
	defer finish()
	_ = server.Shutdown(shutdown)
	return err
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("stopped", "service", "bmkg", "reason", err.Error())
		os.Exit(1)
	}
}
