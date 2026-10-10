package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"atlas/consumers/consumer-a/internal/consumer"
)

func main() {
	cfg, err := consumer.ConfigFromEnv()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", cfg.Name)

	if err != nil {
		log.Error("configuration_invalid", "reason", err.Error())
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := consumer.Run(ctx, cfg, log); err != nil {
		log.Error("stopped", "reason", err.Error())
		os.Exit(1)
	}
}
