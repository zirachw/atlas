package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"atlas/services/bnpb/aggregator/internal/aggregator"
)

func run(log *slog.Logger) error {
	c, e := aggregator.ConfigFromEnv()
	if e != nil {
		return e
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	startup, stop := context.WithTimeout(ctx, 15*time.Second)
	store, e := aggregator.OpenStore(startup, c.DatabaseURL, c.IngestPool, c.QueryPool)
	stop()
	if e != nil {
		return errors.New("canonical store startup failed")
	}
	defer store.Close()
	pending := make(chan aggregator.Publication, c.PublishCapacity)
	worker := &aggregator.PublishWorker{Publisher: &aggregator.RabbitPublisher{URL: c.RabbitMQURL, Timeout: c.PublishTimeout}, Pending: pending, Timeout: c.PublishTimeout, Backoff: c.Backoff, Retries: c.PublishRetries, Log: log}
	var wg sync.WaitGroup
	launch := func(f func(context.Context)) { wg.Add(1); go func() { defer wg.Done(); f(ctx) }() }
	launch(worker.Run)
	for _, source := range []string{"BMKG", "PVMBG"} {
		base, key := c.BMKGURL, c.BMKGKey
		if source == "PVMBG" {
			base, key = c.PVMBGURL, c.PVMBGToken
		}
		transport := &http.Transport{MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: c.SourceTimeout}
		defer transport.CloseIdleConnections()
		poller := &aggregator.Poller{Source: source, BaseURL: base, Credential: key, Interval: c.PollInterval, Timeout: c.SourceTimeout, Backoff: c.Backoff, Retries: c.SourceRetries, Client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, Store: store, Pending: pending, Log: log}
		launch(poller.Run)
	}
	api := &aggregator.API{Store: store, Token: c.InternalToken, Timeout: c.QueryTimeout, Capacity: make(chan struct{}, c.QueryCapacity), Log: log}
	server := &http.Server{Addr: c.Address, Handler: api.Handler(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: c.QueryTimeout + time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	log.Info("started", "service", "aggregator", "address", c.Address)
	select {
	case <-ctx.Done():
	case e = <-done:
		if errors.Is(e, http.ErrServerClosed) {
			e = nil
		}
		cancel()
	}
	shutdown, finish := context.WithTimeout(context.Background(), 5*time.Second)
	defer finish()
	_ = server.Shutdown(shutdown)
	cancel()
	wg.Wait()
	return e
}
func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if e := run(log); e != nil {
		log.Error("stopped", "service", "aggregator", "reason", e.Error())
		os.Exit(1)
	}
}
