package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const exchangeName = "hazard.events"

type Health struct{ ready atomic.Bool }

func (h *Health) Ready() bool { return h.ready.Load() }
func (h *Health) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":{"code":"method_not_allowed"}}`, http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if !h.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"unavailable"}` + "\n"))
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok"}` + "\n"))
	})
}

type hazardMessage struct {
	HazardID string `json:"hazard_id"`
}

func consumeOnce(ctx context.Context, cfg Config, state *ProcessedState, health *Health, log *slog.Logger) error {
	started := time.Now()
	conn, err := amqp.DialConfig(cfg.AMQPURL, amqp.Config{Heartbeat: 10 * time.Second, Dial: func(network, address string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, network, address)
	}})
	if err != nil {
		log.Warn("broker_connect_failed", "service", cfg.Name, "dependency", "rabbitmq", "latency_ms", time.Since(started).Milliseconds())
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err = ch.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil); err != nil {
		return err
	}
	queue, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, cfg.queueArguments())
	if err != nil {
		return err
	}
	if err = ch.QueueBind(queue.Name, "", exchangeName, false, nil); err != nil {
		return err
	}
	prefetch := cfg.Prefetch
	if prefetch == 0 {
		prefetch = 1
	}
	if err = ch.Qos(prefetch, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(queue.Name, cfg.Name, false, false, false, false, nil)
	if err != nil {
		return err
	}
	health.ready.Store(true)
	log.Info("broker_connected", "service", cfg.Name, "queue", queue.Name, "exchange", exchangeName, "dependency", "rabbitmq", "latency_ms", time.Since(started).Milliseconds())
	defer health.ready.Store(false)
	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("broker delivery stream closed")
			}
			var message hazardMessage
			if err := json.Unmarshal(delivery.Body, &message); err != nil || !validHazardID(message.HazardID) {
				log.Warn("message_rejected", "service", cfg.Name, "queue", cfg.Queue, "correlation_id", delivery.CorrelationId, "hazard_id", message.HazardID, "reason", "invalid_message")
				ackStart := time.Now()
				if err := delivery.Nack(false, false); err != nil {
					return err
				}
				log.Info("broker_ack", "service", cfg.Name, "dependency", "rabbitmq", "queue", cfg.Queue, "correlation_id", delivery.CorrelationId, "acknowledged", false, "requeued", false, "latency_ms", time.Since(ackStart).Milliseconds())
				continue
			}
			processed, err := state.Record(message.HazardID)
			if err != nil {
				return err
			}
			if processed {
				log.Info("event_processed", "service", cfg.Name, "queue", cfg.Queue, "hazard_id", message.HazardID, "correlation_id", delivery.CorrelationId)
			} else {
				log.Info("duplicate_delivery", "service", cfg.Name, "queue", cfg.Queue, "hazard_id", message.HazardID, "correlation_id", delivery.CorrelationId)
			}
			ackStart := time.Now()
			if err := delivery.Ack(false); err != nil {
				return err
			}
			log.Info("broker_ack", "service", cfg.Name, "dependency", "rabbitmq", "queue", cfg.Queue, "hazard_id", message.HazardID, "correlation_id", delivery.CorrelationId, "acknowledged", true, "latency_ms", time.Since(ackStart).Milliseconds())
		}
	}
}

func Run(ctx context.Context, cfg Config, log *slog.Logger) error {
	state, err := OpenProcessedState(cfg.StatePath)
	if err != nil {
		return err
	}
	health := &Health{}
	server := &http.Server{Addr: cfg.HealthAddr, Handler: health.Handler(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 15 * time.Second}
	listener, err := net.Listen("tcp", cfg.HealthAddr)
	if err != nil {
		return err
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener); stop() }()
	for runCtx.Err() == nil {
		err := consumeOnce(runCtx, cfg, state, health, log)
		if runCtx.Err() != nil {
			break
		}
		if err != nil {
			log.Warn("consumer_disconnected", "service", cfg.Name, "queue", cfg.Queue, "reason", err.Error())
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-runCtx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
	serveErr := <-serverDone
	if errors.Is(serveErr, http.ErrServerClosed) {
		return nil
	}
	return serveErr
}
