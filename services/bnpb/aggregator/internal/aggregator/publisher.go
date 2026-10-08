package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const Exchange = "hazard.events"

// Called by a single worker. No network waits occur in a poller or read handler.
type RabbitPublisher struct {
	URL     string
	Timeout time.Duration
	conn    *amqp.Connection
	channel *amqp.Channel
	socket  net.Conn
}

func (p *RabbitPublisher) Close() {
	if p.socket != nil {
		_ = p.socket.Close()
	}
	p.conn = nil
	p.channel = nil
	p.socket = nil
}
func (p *RabbitPublisher) connect(ctx context.Context) error {
	if p.conn != nil && !p.conn.IsClosed() {
		return nil
	}
	p.Close()
	deadline, _ := ctx.Deadline()
	conn, e := amqp.DialConfig(p.URL, amqp.Config{Heartbeat: 10 * time.Second, Dial: func(network, address string) (net.Conn, error) {
		d := net.Dialer{Timeout: p.Timeout}
		s, e := d.DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		p.socket = s
		_ = s.SetDeadline(deadline)
		return s, nil
	}})
	if e != nil {
		p.Close()
		return errors.New("broker connection failed")
	}
	p.conn = conn
	ch, e := conn.Channel()
	if e != nil {
		p.Close()
		return errors.New("broker channel failed")
	}
	p.channel = ch
	if e = ch.ExchangeDeclare(Exchange, "fanout", true, false, false, false, nil); e != nil {
		p.Close()
		return errors.New("broker exchange failed")
	}
	if e = ch.Confirm(false); e != nil {
		p.Close()
		return errors.New("broker confirm setup failed")
	}
	return nil
}
func (p *RabbitPublisher) Publish(ctx context.Context, item Publication) error {
	ctx, cancel := context.WithTimeout(ctx, p.Timeout)
	defer cancel()
	if e := p.connect(ctx); e != nil {
		return e
	}
	deadline, _ := ctx.Deadline()
	_ = p.socket.SetDeadline(deadline)
	raw, e := json.Marshal(item.Event)
	if e != nil {
		return e
	}
	// Socket deadlines bound AMQP writes and RPCs as well as confirmation waits.
	confirm, e := p.channel.PublishWithDeferredConfirmWithContext(ctx, Exchange, "", false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, ContentType: "application/json", Type: "HazardEvent", MessageId: fmt.Sprintf("%s:%d", item.Event.HazardID, item.Event.Revision), CorrelationId: item.CorrelationID, Timestamp: time.Now().UTC(), Body: raw,
	})
	if e == nil && confirm != nil {
		var ack bool
		ack, e = confirm.WaitContext(ctx)
		if e == nil && !ack {
			e = errors.New("broker nack")
		}
	} else if e == nil {
		e = errors.New("broker confirm missing")
	}
	if e != nil {
		p.Close()
		return errors.New("publication unconfirmed")
	}
	_ = p.socket.SetDeadline(time.Time{})
	return nil
}

type EventPublisher interface {
	Publish(context.Context, Publication) error
	Close()
}
type PublishWorker struct {
	Publisher        EventPublisher
	Pending          <-chan Publication
	Timeout, Backoff time.Duration
	Retries          int
	Log              *slog.Logger
}

func (w *PublishWorker) send(ctx context.Context, item Publication) bool {
	for attempt := 0; attempt <= w.Retries; attempt++ {
		if ctx.Err() != nil {
			return false
		}
		if attempt > 0 && !pause(ctx, w.Backoff*time.Duration(1<<(attempt-1))) {
			return false
		}
		callCtx, cancel := context.WithTimeout(ctx, w.Timeout)
		start := time.Now()
		e := w.Publisher.Publish(callCtx, item)
		cancel()
		w.Log.Info("broker_call", "service", "aggregator", "correlation_id", item.CorrelationID, "hazard_id", item.Event.HazardID, "revision", item.Event.Revision, "source", item.Event.Source, "dependency", "rabbitmq", "latency_ms", time.Since(start).Milliseconds(), "attempt", attempt+1, "confirmed", e == nil)
		if e == nil {
			return true
		}
	}
	w.Log.Error("publication_dropped", "service", "aggregator", "hazard_id", item.Event.HazardID, "revision", item.Event.Revision, "correlation_id", item.CorrelationID, "reason", "retries_exhausted")
	return false
}
func (w *PublishWorker) Run(ctx context.Context) {
	defer w.Publisher.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-w.Pending:
			if !ok {
				return
			}
			w.send(ctx, item)
		}
	}
}
