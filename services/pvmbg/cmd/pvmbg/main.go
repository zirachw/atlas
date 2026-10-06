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

	"atlas/services/pvmbg/internal/pvmbg"
)

func run(log *slog.Logger) error {
	cfg, err := pvmbg.ConfigFromEnv()
	if err != nil {
		return err
	}
	store, err := pvmbg.LoadSeed()
	if err != nil {
		return err
	}
	controls := pvmbg.NewControls()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go pvmbg.RunGenerator(ctx, store, controls, pvmbg.NewGenerator(pvmbg.Seed, time.Now()), cfg.EventInterval, log)

	api := &pvmbg.API{Store: store, Controls: controls, Token: cfg.Token, Delay: cfg.RandomDelay(), MaxHang: pvmbg.DefaultMaxHang, Log: log}
	server := &http.Server{Addr: cfg.Address, Handler: api.Handler(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: cfg.MaxDelay + 5*time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	log.Info("started", "service", "pvmbg", "address", cfg.Address, "min_delay_ms", cfg.MinDelay.Milliseconds(), "max_delay_ms", cfg.MaxDelay.Milliseconds(), "event_interval", cfg.EventInterval.String(), "seed_reports", len(store.Reports(nil)))
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
		log.Error("stopped", "service", "pvmbg", "reason", err.Error())
		os.Exit(1)
	}
}
