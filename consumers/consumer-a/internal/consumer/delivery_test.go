package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type duplicateHandler struct{ seen chan struct{} }

func (h duplicateHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h duplicateHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "duplicate_delivery" {
		select {
		case h.seen <- struct{}{}:
		default:
		}
	}

	return nil
}

func (h duplicateHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h duplicateHandler) WithGroup(string) slog.Handler { return h }

func TestIntegrationRedeliveryAfterPersistBeforeAck(t *testing.T) {
	url := os.Getenv("CONSUMER_TEST_AMQP_URL")
	if url == "" {
		t.Skip("CONSUMER_TEST_AMQP_URL is not set")
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	queue := fmt.Sprintf("integration.redelivery.%d", time.Now().UnixNano())

	cfg := testConfig(url, "redelivery", queue, filepath.Join(t.TempDir(), "ids.txt"))
	if err := ch.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, cfg.queueArguments()); err != nil {
		t.Fatal(err)
	}
	defer ch.QueueDelete(cfg.Queue, false, false, false)

	if err := ch.QueueBind(cfg.Queue, "", exchangeName, false, nil); err != nil {
		t.Fatal(err)
	}

	message := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		Body:         []byte(`{"hazard_id":"crash-window-1","revision":1}`),
	}
	if err := ch.PublishWithContext(context.Background(), exchangeName, "", false, false, message); err != nil {
		t.Fatal(err)
	}

	crashed, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)

	var delivery amqp.Delivery

	for {
		var ok bool

		delivery, ok, err = crashed.Get(cfg.Queue, false)
		if err != nil {
			t.Fatal(err)
		}

		if ok {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("message not delivered")
		}

		time.Sleep(10 * time.Millisecond)
	}

	if delivery.Redelivered {
		t.Fatal("first delivery already redelivered")
	}

	state, err := OpenProcessedState(cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}

	if fresh, err := state.RecordRevision("crash-window-1", 1); err != nil || !fresh {
		t.Fatalf("record=%v %v", fresh, err)
	}
	// Closing without acknowledgement models the crash after the durable effect.
	if err := crashed.Close(); err != nil {
		t.Fatal(err)
	}

	state, err = OpenProcessedState(cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	duplicate := make(chan struct{}, 1)
	go func() { done <- consumeOnce(ctx, cfg, state, &Health{}, slog.New(duplicateHandler{duplicate})) }()

	select {
	case <-duplicate:
	case <-time.After(5 * time.Second):
		t.Fatal("redelivery not deduplicated after restart")
	}

	cancel()

	if err := <-done; err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(cfg.StatePath)
	if err != nil || strings.Count(string(raw), "crash-window-1:1\n") != 1 {
		t.Fatalf("duplicate stored effect: %q %v", raw, err)
	}

	q, err := ch.QueueDeclarePassive(cfg.Queue, true, false, false, false, nil)
	if err != nil || q.Messages != 0 {
		t.Fatalf("delivery not acknowledged: %+v %v", q, err)
	}
}

func TestIntegrationQueueBoundsAndRetention(t *testing.T) {
	url := os.Getenv("CONSUMER_TEST_AMQP_URL")
	if url == "" {
		t.Skip("CONSUMER_TEST_AMQP_URL is not set")
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	cfg := Config{
		Queue:          fmt.Sprintf("integration.bounds.%d", time.Now().UnixNano()),
		QueueMaxLength: 2,
		QueueMaxBytes:  4096,
		MessageTTL:     time.Second,
	}
	if _, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, cfg.queueArguments()); err != nil {
		t.Fatal(err)
	}
	defer ch.QueueDelete(cfg.Queue, false, false, false)

	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	for i := 0; i < 3; i++ {
		message := amqp.Publishing{DeliveryMode: amqp.Persistent, Body: []byte("bounded-event")}
		if err := ch.PublishWithContext(context.Background(), "", cfg.Queue, false, false, message); err != nil {
			t.Fatal(err)
		}

		select {
		case confirmation := <-confirms:
			if confirmation.Ack != (i < 2) {
				t.Fatalf("publish %d ack=%v", i, confirmation.Ack)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no publisher confirm")
		}
	}

	q, err := ch.QueueDeclarePassive(cfg.Queue, true, false, false, false, nil)
	if err != nil || q.Messages != 2 {
		t.Fatalf("queue exceeds bound: %+v %v", q, err)
	}

	time.Sleep(1200 * time.Millisecond)

	if _, ok, err := ch.Get(cfg.Queue, true); err != nil || ok {
		t.Fatalf("expired event delivered: ok=%v err=%v", ok, err)
	}
}
