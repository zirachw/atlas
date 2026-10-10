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

// Called by a single worker. No network waits occur in a poller or read handler.
type RabbitPublisher struct {
	URL     string
	Timeout time.Duration
	conn    *amqp.Connection
	channel *amqp.Channel
	socket  net.Conn
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

const Exchange = "hazard.events"

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

	dial := func(network, address string) (net.Conn, error) {
		d := net.Dialer{Timeout: p.Timeout}

		s, err := d.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}

		p.socket = s
		_ = s.SetDeadline(deadline)

		return s, nil
	}

	conn, err := amqp.DialConfig(p.URL, amqp.Config{Heartbeat: 10 * time.Second, Dial: dial})
	if err != nil {
		p.Close()
		return errors.New("broker connection failed")
	}

	p.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		p.Close()
		return errors.New("broker channel failed")
	}

	p.channel = ch
	if err = ch.ExchangeDeclare(Exchange, "fanout", true, false, false, false, nil); err != nil {
		p.Close()
		return errors.New("broker exchange failed")
	}

	if err = ch.Confirm(false); err != nil {
		p.Close()
		return errors.New("broker confirm setup failed")
	}

	return nil
}

func (p *RabbitPublisher) Publish(ctx context.Context, item Publication) error {
	ctx, cancel := context.WithTimeout(ctx, p.Timeout)
	defer cancel()

	if err := p.connect(ctx); err != nil {
		return err
	}

	deadline, _ := ctx.Deadline()
	_ = p.socket.SetDeadline(deadline)

	raw, err := json.Marshal(item.Event)
	if err != nil {
		return err
	}
	// Socket deadlines bound AMQP writes and RPCs as well as confirmation waits.
	message := amqp.Publishing{
		DeliveryMode:  amqp.Persistent,
		ContentType:   "application/json",
		Type:          "HazardEvent",
		MessageId:     fmt.Sprintf("%s:%d", item.Event.HazardID, item.Event.Revision),
		CorrelationId: item.CorrelationID,
		Timestamp:     time.Now().UTC(),
		Body:          raw,
	}

	confirm, err := p.channel.PublishWithDeferredConfirmWithContext(ctx, Exchange, "", false, false, message)
	if err == nil && confirm != nil {
		var ack bool

		ack, err = confirm.WaitContext(ctx)
		if err == nil && !ack {
			err = errors.New("broker nack")
		}
	} else if err == nil {
		err = errors.New("broker confirm missing")
	}

	if err != nil {
		p.Close()
		return errors.New("publication unconfirmed")
	}

	_ = p.socket.SetDeadline(time.Time{})

	return nil
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
		err := w.Publisher.Publish(callCtx, item)

		cancel()
		w.Log.Info(
			"broker_call",
			"correlation_id", item.CorrelationID,
			"hazard_id", item.Event.HazardID,
			"revision", item.Event.Revision,
			"source", item.Event.Source,
			"dependency", "rabbitmq",
			"latency_ms", time.Since(start).Milliseconds(),
			"attempt", attempt+1,
			"confirmed", err == nil,
		)

		if err == nil {
			return true
		}
	}

	w.Log.Error(
		"publication_dropped",
		"hazard_id", item.Event.HazardID,
		"revision", item.Event.Revision,
		"correlation_id", item.CorrelationID,
		"reason", "retries_exhausted",
	)

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
