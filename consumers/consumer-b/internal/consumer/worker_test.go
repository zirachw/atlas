package consumer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestIntegrationExistingQueueKeepsBacklog(t *testing.T) {
	url := os.Getenv("CONSUMER_TEST_AMQP_URL")
	if url == "" {
		t.Skip("CONSUMER_TEST_AMQP_URL is not set")
	}

	for _, tc := range []struct {
		name     string
		args     amqp.Table
		newQueue bool
	}{
		{"existing_with_different_limits", amqp.Table{
			"x-max-length": int64(100), "x-max-length-bytes": int64(8192),
			"x-message-ttl": int64(600000), "x-overflow": "reject-publish",
		}, false},
		{"new_queue", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
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

			queue := fmt.Sprintf("integration.existing.%d", time.Now().UnixNano())

			cfg := testConfig(url, "existing", queue, filepath.Join(t.TempDir(), "ids.txt"))
			if !tc.newQueue {
				if _, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, tc.args); err != nil {
					t.Fatal(err)
				}
			}
			defer ch.QueueDelete(cfg.Queue, false, false, false)

			if err := ch.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil); err != nil {
				t.Fatal(err)
			}

			if !tc.newQueue {
				if err := ch.QueueBind(cfg.Queue, "", exchangeName, false, nil); err != nil {
					t.Fatal(err)
				}
			}

			if err := ch.Confirm(false); err != nil {
				t.Fatal(err)
			}

			confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
			publish := func(id string) {
				t.Helper()

				if err := ch.PublishWithContext(context.Background(), exchangeName, "", false, false, amqp.Publishing{
					DeliveryMode: amqp.Persistent, ContentType: "application/json",
					Body: []byte(fmt.Sprintf(`{"hazard_id":%q,"revision":1}`, id)),
				}); err != nil {
					t.Fatal(err)
				}

				select {
				case confirmation := <-confirms:
					if !confirmation.Ack {
						t.Fatal("backlog publication rejected")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("no publisher confirmation")
				}
			}

			if !tc.newQueue {
				// Desired new limits must not expire or truncate retained backlog.
				cfg.QueueMaxLength, cfg.MessageTTL = 1, time.Second

				for _, id := range []string{"already-processed", "backlog-1", "backlog-2"} {
					publish(id)
				}

				time.Sleep(1200 * time.Millisecond)

				q, err := ch.QueueDeclarePassive(cfg.Queue, true, false, false, false, nil)
				if err != nil || q.Messages != 3 {
					t.Fatalf("backlog=%+v err=%v", q, err)
				}
			}

			state, err := OpenProcessedState(cfg.StatePath)
			if err != nil {
				t.Fatal(err)
			}

			if _, err := state.RecordRevision("already-processed", 1); err != nil {
				t.Fatal(err)
			}
			// Reload the ledger as a restarted process would.
			state, err = OpenProcessedState(cfg.StatePath)
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)

			health := &Health{}
			go func() { done <- consumeOnce(ctx, cfg, state, health, slog.New(slog.NewJSONHandler(io.Discard, nil))) }()

			defer func() {
				cancel()

				select {
				case err := <-done:
					if err != nil {
						t.Errorf("consumer: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("consumer did not stop")
				}
			}()

			waitFor := func(ids ...string) {
				t.Helper()

				deadline := time.Now().Add(5 * time.Second)
				for time.Now().Before(deadline) {
					select {
					case err := <-done:
						// Keep the result for deferred shutdown verification.
						done <- err

						t.Fatalf("consumer failed before processing backlog: %v", err)
					default:
					}

					if !health.Ready() {
						time.Sleep(10 * time.Millisecond)
						continue
					}

					raw, err := os.ReadFile(cfg.StatePath)

					complete := err == nil && health.Ready()
					for _, id := range ids {
						complete = complete && strings.Count(string(raw), id+":1\n") == 1
					}

					q, err := ch.QueueDeclarePassive(cfg.Queue, true, false, false, false, nil)
					if err != nil {
						t.Fatal(err)
					}

					if complete && q.Messages == 0 {
						return
					}

					time.Sleep(10 * time.Millisecond)
				}

				t.Fatal("consumer did not process retained events")
			}
			if tc.newQueue {
				waitFor()

				for _, id := range []string{"already-processed", "backlog-1", "backlog-2"} {
					publish(id)
				}
			}

			waitFor("already-processed", "backlog-1", "backlog-2")
			publish("live-event")
			waitFor("already-processed", "backlog-1", "backlog-2", "live-event")

			wantArgs := tc.args
			if tc.newQueue {
				wantArgs = cfg.queueArguments()
			}

			if _, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, wantArgs); err != nil {
				t.Fatalf("queue properties changed: %v", err)
			}
		})
	}
}
