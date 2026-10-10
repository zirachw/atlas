package aggregator

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type markFailureStore struct{ *memoryStore }

func brokerChannel(t *testing.T) *amqp.Channel {
	t.Helper()

	url := os.Getenv("TEST_RABBITMQ_URL")
	if url == "" {
		t.Skip("run make integration")
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { conn.Close() })

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	if err := ch.ExchangeDeclare(Exchange, "fanout", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}

	return ch
}

func bindQueue(t *testing.T, ch *amqp.Channel, args amqp.Table) string {
	t.Helper()

	q, err := ch.QueueDeclare("integration.outbox."+correlationID(), true, false, false, false, args)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { ch.QueueDelete(q.Name, false, false, false) })

	if err := ch.QueueBind(q.Name, "", Exchange, false, nil); err != nil {
		t.Fatal(err)
	}

	return q.Name
}

func TestIntegrationUnroutablePublicationRetainedUntilSubscriberBinds(t *testing.T) {
	s := integrationStore(t)
	ch := brokerChannel(t)
	ctx := context.Background()

	h, _ := MapVolcanic(fixture(t, "volcanic-v1.json")[0], time.Now())
	if _, err := s.Save(ctx, "PVMBG", []HazardEvent{h}, &h.OccurredAt, time.Now()); err != nil {
		t.Fatal(err)
	}

	items, err := s.PendingPublications(ctx, 1)
	if err != nil || len(items) != 1 {
		t.Fatal(items, err)
	}

	publisher := &RabbitPublisher{URL: os.Getenv("TEST_RABBITMQ_URL"), Timeout: time.Second}
	defer publisher.Close()

	if err := publisher.Publish(ctx, items[0]); err == nil || err.Error() != "publication unroutable" {
		t.Fatal("empty exchange was treated as delivered", err)
	}

	queue := bindQueue(t, ch, nil)
	w := &PublishWorker{Publisher: publisher, Store: s, Timeout: time.Second, Backoff: time.Millisecond, Log: testLog()}
	run, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() { w.Run(run); close(done) }()

	waitFor(t, func() bool {
		pending, err := s.PendingPublications(ctx, 1)
		return err == nil && len(pending) == 0
	})
	cancel()
	<-done

	msg, ok, err := ch.Get(queue, true)
	if err != nil || !ok || msg.MessageId != h.HazardID+":1" {
		t.Fatal("subscriber did not receive retained publication", ok, err)
	}

	t.Log("no bindings returns an error; outbox relay confirms and removes pending row only after a subscriber binds")
}

func TestIntegrationBrokerFailureDoesNotExhaustDurableWork(t *testing.T) {
	s := integrationStore(t)
	ch := brokerChannel(t)
	queue := bindQueue(t, ch, nil)
	ctx := context.Background()

	h, _ := MapVolcanic(fixture(t, "volcanic-v1.json")[0], time.Now())
	if _, err := s.Save(ctx, "PVMBG", []HazardEvent{h}, &h.OccurredAt, time.Now()); err != nil {
		t.Fatal(err)
	}

	items, _ := s.PendingPublications(ctx, 1)
	unavailable := &RabbitPublisher{URL: "amqp://127.0.0.1:1/", Timeout: 100 * time.Millisecond}

	w := &PublishWorker{
		Publisher: unavailable, Store: s, Timeout: time.Second,
		Backoff: time.Millisecond, Retries: 2, Log: testLog(),
	}
	if w.send(ctx, items[0]) {
		t.Fatal("unavailable broker confirmed")
	}

	unavailable.Close()

	if pending, err := s.PendingPublications(ctx, 1); err != nil || len(pending) != 1 {
		t.Fatal("exhausted retries discarded pending work", pending, err)
	}

	// Recreate the worker after recovery; it reads SQL rather than a previous process's channel.
	w.Publisher = &RabbitPublisher{URL: os.Getenv("TEST_RABBITMQ_URL"), Timeout: time.Second}
	run, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() { w.Run(run); close(done) }()

	waitFor(t, func() bool {
		pending, err := s.PendingPublications(ctx, 1)
		return err == nil && len(pending) == 0
	})
	cancel()
	<-done

	if msg, ok, err := ch.Get(queue, true); err != nil || !ok || msg.MessageId != h.HazardID+":1" {
		t.Fatal("broker recovery lost snapshot", ok, err)
	}

	t.Log("three failed broker attempts retain outbox; fresh relay after recovery delivers the original snapshot")
}

func (s markFailureStore) ConfirmPublication(context.Context, Publication) error {
	return errors.New("database unavailable after broker confirm")
}

func TestOutboxMarkFailureRetainsConfirmedSnapshot(t *testing.T) {
	s := newMemoryStore()
	s.outbox = []Publication{{Event: HazardEvent{HazardID: "original", Revision: 1}}}
	p := &publisherStub{}
	w := &PublishWorker{Publisher: p, Store: markFailureStore{s}, Timeout: time.Second, Log: testLog()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() { w.Run(ctx); close(done) }()

	waitFor(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()

		return len(p.items) > 0
	})
	cancel()
	<-done

	if len(s.outbox) != 1 {
		t.Fatal("mark failure lost the confirmed revision")
	}
}
