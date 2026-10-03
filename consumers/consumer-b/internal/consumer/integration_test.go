package consumer

import (
	"context"
	"crypto/rand"
	"encoding/json"
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

func TestIntegrationIndependentQueuesAndRestartCatchup(t *testing.T) {
	url := os.Getenv("CONSUMER_TEST_AMQP_URL")
	if url == "" {
		t.Skip("CONSUMER_TEST_AMQP_URL is not set")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	var suffix [5]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	queueA, queueB := fmt.Sprintf("integration.a.%x", suffix), fmt.Sprintf("integration.b.%x", suffix)
	if _, err := ch.QueueDeclare(queueA, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.QueueDeclare(queueB, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := ch.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := ch.QueueBind(queueA, "", exchangeName, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := ch.QueueBind(queueB, "", exchangeName, false, nil); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = ch.QueueDelete(queueA, false, false, false)
		_, _ = ch.QueueDelete(queueB, false, false, false)
		_ = ch.Close()
		_ = conn.Close()
	}()
	base := t.TempDir()
	cfgA := Config{AMQPURL: url, Name: "integration-a", Queue: queueA, StatePath: filepath.Join(base, "a", "ids.txt")}
	cfgB := Config{AMQPURL: url, Name: "integration-b", Queue: queueB, StatePath: filepath.Join(base, "b", "ids.txt")}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	healthA, healthB := &Health{}, &Health{}
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	stateA, err := OpenProcessedState(cfgA.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	stateB, err := OpenProcessedState(cfgB.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	go func() { doneA <- consumeOnce(ctxA, cfgA, stateA, healthA, log) }()
	go func() { doneB <- consumeOnce(ctxB, cfgB, stateB, healthB, log) }()
	waitFor := func(check func() bool, description string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if check() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timeout waiting for %s", description)
	}
	waitFor(func() bool { return healthA.Ready() && healthB.Ready() }, "both consumers to bind")
	publish := func(id string) {
		t.Helper()
		raw, _ := json.Marshal(map[string]string{"hazard_id": id})
		if err := ch.PublishWithContext(context.Background(), exchangeName, "", false, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "application/json", CorrelationId: "integration-correlation", Body: raw}); err != nil {
			t.Fatal(err)
		}
	}
	contains := func(path, id string) bool {
		raw, err := os.ReadFile(path)
		return err == nil && strings.Contains(string(raw), id+"\n")
	}
	publish("integration-1")
	waitFor(func() bool {
		return contains(cfgA.StatePath, "integration-1") && contains(cfgB.StatePath, "integration-1")
	}, "both independent queues to process the first event")
	cancelA()
	select {
	case <-doneA:
	case <-time.After(5 * time.Second):
		t.Fatal("Consumer A did not stop")
	}
	waitFor(func() bool { return !healthA.Ready() }, "Consumer A to disconnect")
	publish("integration-2")
	waitFor(func() bool { return contains(cfgB.StatePath, "integration-2") }, "Consumer B to continue while A is stopped")
	stateA, err = OpenProcessedState(cfgA.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	ctxA2, cancelA2 := context.WithCancel(context.Background())
	defer cancelA2()
	healthA2 := &Health{}
	doneA2 := make(chan error, 1)
	go func() { doneA2 <- consumeOnce(ctxA2, cfgA, stateA, healthA2, log) }()
	waitFor(func() bool { return contains(cfgA.StatePath, "integration-2") }, "Consumer A to catch up after restart")
	cancelA2()
	select {
	case <-doneA2:
	case <-time.After(5 * time.Second):
		t.Fatal("restarted Consumer A did not stop")
	}
	cancelB()
	select {
	case <-doneB:
	case <-time.After(5 * time.Second):
		t.Fatal("Consumer B did not stop")
	}
}
