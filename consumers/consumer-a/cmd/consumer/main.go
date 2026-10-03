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
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := consumer.ConfigFromEnv()
	if err != nil {
		log.Error("configuration_invalid", "service", "consumer-a", "reason", err.Error())
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := consumer.Run(ctx, cfg, log); err != nil {
		log.Error("stopped", "service", "consumer-a", "reason", err.Error())
		os.Exit(1)
	}
}
