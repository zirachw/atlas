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

type Health struct{ ready atomic.Bool }

type hazardMessage struct {
	HazardID string `json:"hazard_id"`
	Revision int64  `json:"revision"`
}

const exchangeName = "hazard.events"

func (h *Health) Ready() bool { return h.ready.Load() }

func (h *Health) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(
				w,
				http.StatusMethodNotAllowed,
				map[string]any{"error": map[string]string{"code": "method_not_allowed"}},
			)

			return
		}

		if !h.Ready() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

func Run(ctx context.Context, cfg Config, log *slog.Logger) error {
	state, err := OpenProcessedState(cfg.StatePath)
	if err != nil {
		return err
	}

	health := &Health{}
	server := &http.Server{
		Addr:              cfg.HealthAddr,
		Handler:           health.Handler(),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       3 * time.Second,
		WriteTimeout:      3 * time.Second,
		IdleTimeout:       15 * time.Second,
	}

	listener, err := net.Listen("tcp", cfg.HealthAddr)
	if err != nil {
		return err
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	serverDone := make(chan error, 1)

	go func() {
		serverDone <- server.Serve(listener)

		stop()
	}()

	for runCtx.Err() == nil {
		err := consumeOnce(runCtx, cfg, state, health, log)
		if runCtx.Err() != nil {
			break
		}

		if err != nil {
			log.Warn("consumer_disconnected", "queue", cfg.Queue, "reason", err.Error())
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

func consumeOnce(ctx context.Context, cfg Config, state *ProcessedState, health *Health, log *slog.Logger) error {
	conn, deliveries, err := subscribe(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer conn.Close()

	health.ready.Store(true)
	defer health.ready.Store(false)

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("broker delivery stream closed")
			}

			if err := handle(cfg, state, delivery, log); err != nil {
				return err
			}
		}
	}
}

func subscribe(ctx context.Context, cfg Config, log *slog.Logger) (*amqp.Connection, <-chan amqp.Delivery, error) {
	started := time.Now()
	dial := func(network, address string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, network, address)
	}

	conn, err := amqp.DialConfig(cfg.AMQPURL, amqp.Config{Heartbeat: 10 * time.Second, Dial: dial})
	if err != nil {
		log.Warn("broker_connect_failed", "dependency", "rabbitmq", "latency_ms", time.Since(started).Milliseconds())
		return nil, nil, err
	}

	deliveries, err := declareAndConsume(conn, cfg)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}

	log.Info("broker_connected",
		"queue", cfg.Queue,
		"exchange", exchangeName,
		"dependency", "rabbitmq",
		"latency_ms", time.Since(started).Milliseconds(),
	)

	return conn, deliveries, nil
}

// An existing queue keeps its arguments and backlog, so only a missing queue gets the configured limits.
func declareAndConsume(conn *amqp.Connection, cfg Config) (<-chan amqp.Delivery, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if err = ch.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil); err != nil {
		return nil, err
	}

	if _, err = ch.QueueDeclarePassive(cfg.Queue, true, false, false, false, nil); err != nil {
		var brokerErr *amqp.Error
		if !errors.As(err, &brokerErr) || brokerErr.Code != amqp.NotFound {
			return nil, err
		}

		// A missing queue closes the channel, so the queue is created on a new one.
		if ch, err = conn.Channel(); err != nil {
			return nil, err
		}

		if _, err = ch.QueueDeclare(cfg.Queue, true, false, false, false, cfg.queueArguments()); err != nil {
			return nil, err
		}
	}

	if err = ch.QueueBind(cfg.Queue, "", exchangeName, false, nil); err != nil {
		return nil, err
	}

	if err = ch.Qos(cfg.Prefetch, 0, false); err != nil {
		return nil, err
	}

	return ch.Consume(cfg.Queue, cfg.Name, false, false, false, false, nil)
}

// handle records the event before acknowledging it, so a crash between the two causes a duplicate, not a loss.
func handle(cfg Config, state *ProcessedState, delivery amqp.Delivery, log *slog.Logger) error {
	var message hazardMessage

	err := json.Unmarshal(delivery.Body, &message)
	if err != nil || !validHazardID(message.HazardID) || message.Revision < 1 {
		log.Warn("message_rejected",
			"queue", cfg.Queue,
			"correlation_id", delivery.CorrelationId,
			"hazard_id", message.HazardID,
			"revision", message.Revision,
			"reason", "invalid_message",
		)

		return acknowledge(cfg, delivery, message, false, log)
	}

	processed, err := state.RecordRevision(message.HazardID, message.Revision)
	if err != nil {
		return err
	}

	event := "event_processed"
	if !processed {
		event = "duplicate_delivery"
	}

	log.Info(event,
		"queue", cfg.Queue,
		"hazard_id", message.HazardID,
		"revision", message.Revision,
		"correlation_id", delivery.CorrelationId,
	)

	return acknowledge(cfg, delivery, message, true, log)
}

// acknowledge acks a handled delivery, or drops an invalid one without requeueing it.
func acknowledge(cfg Config, delivery amqp.Delivery, message hazardMessage, ok bool, log *slog.Logger) error {
	start := time.Now()

	var err error
	if ok {
		err = delivery.Ack(false)
	} else {
		err = delivery.Nack(false, false)
	}

	if err != nil {
		return err
	}

	log.Info("broker_ack",
		"dependency", "rabbitmq",
		"queue", cfg.Queue,
		"hazard_id", message.HazardID,
		"revision", message.Revision,
		"correlation_id", delivery.CorrelationId,
		"acknowledged", ok,
		"latency_ms", time.Since(start).Milliseconds(),
	)

	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
