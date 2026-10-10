package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	amqp "github.com/rabbitmq/amqp091-go"
)

func integrationStore(t *testing.T) *Store {
	t.Helper()

	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL or run make integration")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := OpenStore(ctx, url, 2, 4)
	if err != nil {
		t.Fatal(err)
	}

	const reset = `
		TRUNCATE hazard_events, publication_outbox;
		UPDATE source_progress SET checkpoint = NULL, last_success_at = NULL, stale_since = NULL`
	if _, err = s.ingest.Exec(ctx, reset); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(s.Close)

	return s
}

func TestIntegrationStoreCommitRollbackRestartAndOwnership(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	h, err := MapVolcanic(fixture(t, "volcanic-v1.json")[0], now)
	if err != nil {
		t.Fatal(err)
	}

	created, err := s.Save(ctx, "PVMBG", []HazardEvent{h}, &h.OccurredAt, now)
	if err != nil || len(created) != 1 {
		t.Fatal(created, err)
	}

	original := h.IngestedAt
	h.IngestedAt = now.Add(time.Hour)
	h.Attributes["confidence_level"] = json.RawMessage("0.8")

	created, err = s.Save(ctx, "PVMBG", []HazardEvent{h}, &h.OccurredAt, now)
	if err != nil || len(created) != 1 || created[0].Revision != 2 {
		t.Fatal(created, err)
	}

	h.Attributes["confidence_level"] = json.RawMessage(" 0.8 ")

	unchanged, err := s.Save(ctx, "PVMBG", []HazardEvent{h}, &h.OccurredAt, now)
	if err != nil || len(unchanged) != 0 {
		t.Fatal("unchanged JSON published", unchanged, err)
	}

	newer, err := MapVolcanic(fixture(t, "volcanic-v2.json")[0], now)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = s.Save(ctx, "PVMBG", []HazardEvent{newer}, &newer.OccurredAt, now); err != nil {
		t.Fatal(err)
	}

	data, err := s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
	if err != nil || len(data.Data) != 2 {
		t.Fatal(data, err)
	}

	if data.Data[1].HazardID != h.HazardID ||
		!data.Data[1].IngestedAt.Truncate(time.Microsecond).Equal(original.Truncate(time.Microsecond)) {
		t.Fatal("duplicate changed identity or ingest time")
	}
	// Roll back both an earlier inserted row and its checkpoint on a later failure.
	next := newer
	next.SourceRefID = "rollback-valid"
	next.HazardID = hazardID(next.Source, next.SourceRefID)
	next.OccurredAt = now
	bad := next
	bad.SourceRefID = "rollback-invalid"
	bad.HazardID = hazardID(bad.Source, bad.SourceRefID)

	bad.Severity = "BROKEN"
	if _, err = s.Save(ctx, "PVMBG", []HazardEvent{next, bad}, &now, now); err == nil {
		t.Fatal("invalid SQL constraint succeeded")
	}

	checkpoint, err := s.Checkpoint(ctx, "PVMBG")
	if err != nil || !checkpoint.Equal(newer.OccurredAt) {
		t.Fatal(checkpoint, err)
	}

	data, err = s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
	if err != nil || len(data.Data) != 2 {
		t.Fatal("rollback left row", data, err)
	}

	items, err := s.PendingPublications(ctx, 100)
	if err != nil || len(items) != 3 {
		t.Fatal("outbox rollback or changed revision count", items, err)
	}

	for _, item := range items {
		if item.Event.HazardID == h.HazardID && item.Event.Revision == 1 {
			if _, exists := item.Event.Attributes["confidence_level"]; exists {
				t.Fatal("old outbox snapshot was reconstructed from the newest revision")
			}
		}
	}

	restarted, err := OpenStore(ctx, os.Getenv("TEST_DATABASE_URL"), 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()

	cp, err := restarted.Checkpoint(ctx, "PVMBG")
	if err != nil || !cp.Equal(*checkpoint) {
		t.Fatal("restart lost progress", cp, err)
	}

	data, err = restarted.Read(ctx, Query{Source: "PVMBG", Limit: 100})
	if err != nil || data.Sources[0].Status != "stale" || len(data.Data) != 2 {
		t.Fatal("restart freshness", data, err)
	}
	// The role used by Auth must not connect to Canonical Store.
	authURL := os.Getenv("TEST_AUTH_CANONICAL_URL")
	if authURL == "" {
		t.Fatal("missing ownership test URL")
	}

	conn, err := pgx.Connect(ctx, authURL)
	if err == nil {
		conn.Close(ctx)
		t.Fatal("Auth role gained Canonical Store access")
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("expected permission denied, got %T %v", err, err)
	}

	sample, _ := json.Marshal(data)
	t.Logf("restart_snapshot=%s checkpoint=%s", sample, cp.Format(time.RFC3339Nano))
	t.Log("verified unique identity, rollback, checkpoint restart, JSONB and denied non-owner connection")
}

func TestIntegrationLivePollingMappingFreshnessAndReads(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	seismic := fixture(t, "seismic.json")
	old := fixture(t, "volcanic-v1.json")
	newer := fixture(t, "volcanic-v2.json")
	warnings := fixture(t, "warnings.json")

	var (
		schema, outage, delay, lateWarning atomic.Bool
		bmkgCalls                          atomic.Int32
	)

	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bmkgCalls.Add(1)

		if r.Header.Get("X-BMKG-Key") != "test-BMKG" || r.Header.Get("Authorization") != "" {
			w.WriteHeader(401)
			return
		}

		if r.URL.Path == "/tsunami-warnings" {
			if lateWarning.Load() {
				_ = json.NewEncoder(w).Encode(warnings)
			} else {
				w.Write([]byte("[]"))
			}

			return
		}

		records := []Object{}

		for _, o := range seismic {
			when, _ := timestamp(o, "occurred_at")
			if r.URL.Query().Get("since") == "" || when.Format(time.RFC3339) >= r.URL.Query().Get("since") {
				records = append(records, o)
			}
		}

		_ = json.NewEncoder(w).Encode(records)
	}))
	defer fast.Close()

	slowEntered := make(chan struct{}, 1)

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-PVMBG" || r.Header.Get("X-BMKG-Key") != "" {
			w.WriteHeader(401)
			return
		}

		if outage.Load() {
			w.WriteHeader(503)
			return
		}

		if delay.Load() {
			select {
			case slowEntered <- struct{}{}:
			default:
			}

			select {
			case <-r.Context().Done():
				return
			case <-time.After(3 * time.Second):
			}
		}

		records := old
		if schema.Load() {
			records = newer
		}

		_ = json.NewEncoder(w).Encode(records)
	}))
	defer slow.Close()

	pending := make(chan Publication, 64)
	a := newPoller("BMKG", fast.URL, s, pending)
	b := newPoller("PVMBG", slow.URL, s, pending)
	b.Timeout = 4 * time.Second

	if err := a.Poll(ctx, "seed-bmkg"); err != nil {
		t.Fatal(err)
	}

	if err := b.Poll(ctx, "seed-pvmbg"); err != nil {
		t.Fatal(err)
	}

	oldIDs := map[string]bool{}
	for len(pending) > 0 {
		oldIDs[(<-pending).Event.HazardID] = true
	}

	schema.Store(true)
	lateWarning.Store(true)

	if err := a.Poll(ctx, "late-warning"); err != nil {
		t.Fatal(err)
	}

	if err := b.Poll(ctx, "live-schema"); err != nil {
		t.Fatal(err)
	}

	if len(pending) != 2 {
		t.Fatal("missing late warning revision or new volcanic event", len(pending))
	}

	update := <-pending
	if !oldIDs[update.Event.HazardID] || update.Event.Revision != 2 || update.Event.Severity != "AWAS" {
		t.Fatal("late warning publication", update)
	}

	<-pending

	if err := a.Poll(ctx, "same-warning"); err != nil {
		t.Fatal(err)
	}

	if len(pending) != 0 {
		t.Fatal("unchanged warning republished")
	}

	data, err := s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
	if err != nil || len(data.Data) != 2 {
		t.Fatal(data, err)
	}

	if _, ok := data.Data[1].Attributes["confidence_level"]; ok {
		t.Fatal("old record rewritten with new shape")
	}

	if string(data.Data[0].Attributes["confidence_level"]) != "0.85" {
		t.Fatal("new field lost")
	}

	quakes, err := s.Read(ctx, Query{Source: "BMKG", Severity: "AWAS", Limit: 100})
	if err != nil || len(quakes.Data) != 1 || !oldIDs[quakes.Data[0].HazardID] {
		t.Fatal("late warning or identity failed", quakes, err)
	}

	api := (&API{Store: s, Token: "test-internal", Timeout: time.Second, Capacity: make(chan struct{}, 64), Log: testLog()}).Handler()

	callsBefore := bmkgCalls.Load()
	if request(api, "/internal/hazards?source=BMKG", "test-internal").Code != 200 || bmkgCalls.Load() != callsBefore {
		t.Fatal("read polled source")
	}
	// Keep a real three-second PVMBG call in flight while BMKG polling and reads proceed.
	delay.Store(true)

	done := make(chan error, 1)
	go func() { done <- b.Poll(ctx, "slow-three-seconds") }()

	<-slowEntered

	start := time.Now()

	if err := a.Poll(ctx, "isolated-bmkg"); err != nil {
		t.Fatal(err)
	}

	if w := request(api, "/internal/hazards?source=BMKG", "test-internal"); w.Code != 200 {
		t.Fatal(w.Code)
	}

	if time.Since(start) > 300*time.Millisecond {
		t.Fatal("BMKG blocked on PVMBG")
	}

	if err := <-done; err != nil {
		t.Fatal(err)
	}

	delay.Store(false)
	outage.Store(true)

	runCtx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() { defer wg.Done(); b.Run(runCtx) }()

	waitFor(t, func() bool {
		r, err := s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
		return err == nil && r.Sources[0].Status == "stale" && r.Sources[0].StaleSince != nil
	})

	stale, err := s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}

	snapshot, _ := json.Marshal(stale)
	t.Logf("outage_snapshot=%s", snapshot)

	if request(api, "/internal/hazards?source=PVMBG", "test-internal").Code != 200 {
		t.Fatal("stale data unusable")
	}

	outage.Store(false)
	waitFor(t, func() bool {
		r, err := s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
		return err == nil && r.Sources[0].Status == "healthy" && r.Sources[0].StaleSince == nil
	})
	cancel()
	wg.Wait()
	// Empty source during failure is explicitly unavailable, without generic 500.
	if _, err = s.ingest.Exec(ctx, "DELETE FROM hazard_events WHERE source='PVMBG'"); err != nil {
		t.Fatal(err)
	}

	if err = s.Failure(ctx, "PVMBG", time.Now()); err != nil {
		t.Fatal(err)
	}

	if request(api, "/internal/hazards?source=PVMBG", "test-internal").Code != 503 ||
		request(api, "/internal/hazards", "test-internal").Code != 200 ||
		request(api, "/internal/hazards?source=BMKG", "test-internal").Code != 200 {
		t.Fatal("unavailable source damaged mixed/seismic reads")
	}

	t.Log("verified live additive schema, late warning, three-second isolation, stored reads, stale outage and automatic recovery")
}

func TestIntegrationRabbitFanoutConfirmAndReconnect(t *testing.T) {
	broker := os.Getenv("TEST_RABBITMQ_URL")
	if broker == "" {
		t.Skip("set TEST_RABBITMQ_URL or run make integration")
	}

	conn, err := amqp.Dial(broker)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	if err = ch.ExchangeDeclare(Exchange, "fanout", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}

	queues := []string{"integration.a." + correlationID(), "integration.b." + correlationID()}
	for _, name := range queues {
		args := amqp.Table{
			"x-max-length":       int32(10000),
			"x-max-length-bytes": int32(100 << 20),
			"x-overflow":         "reject-publish",
		}

		q, err := ch.QueueDeclare(name, true, false, false, false, args)
		if err != nil {
			t.Fatal(err)
		}

		if err = ch.QueueBind(q.Name, "", Exchange, false, nil); err != nil {
			t.Fatal(err)
		}
		defer ch.QueueDelete(q.Name, false, false, false)
	}

	p := &RabbitPublisher{URL: broker, Timeout: time.Second}
	defer p.Close()

	h, err := MapVolcanic(fixture(t, "volcanic-v1.json")[0], time.Now())
	if err != nil {
		t.Fatal(err)
	}

	item := Publication{h, "broker-test"}
	if err = p.Publish(context.Background(), item); err != nil {
		t.Fatal(err)
	}

	for _, q := range queues {
		msg, ok, err := ch.Get(q, false)
		if err != nil || !ok {
			t.Fatal(q, ok, err)
		}

		var got HazardEvent
		if err = json.Unmarshal(msg.Body, &got); err != nil {
			t.Fatal(err)
		}

		if got.HazardID != h.HazardID || msg.MessageId != h.HazardID+":1" || msg.CorrelationId != "broker-test" ||
			msg.DeliveryMode != amqp.Persistent {
			t.Fatal("message identity or metadata lost")
		}

		if err = msg.Ack(false); err != nil {
			t.Fatal(err)
		}
	}
	// Publish an updated snapshot with a distinct revision identity to both queues.
	item.Event.Revision = 2

	item.Event.Severity = "AWAS"
	if err = p.Publish(context.Background(), item); err != nil {
		t.Fatal(err)
	}

	for _, q := range queues {
		msg, ok, err := ch.Get(q, true)
		if err != nil || !ok {
			t.Fatal(q, ok, err)
		}

		var got HazardEvent
		if err = json.Unmarshal(msg.Body, &got); err != nil {
			t.Fatal(err)
		}

		if got.HazardID != h.HazardID || got.Revision != 2 || got.Severity != "AWAS" ||
			msg.MessageId != h.HazardID+":2" {
			t.Fatal("update metadata or payload lost", got)
		}
	}
	// Drop the actual network connection, then publish through a reconnected channel.
	_ = p.socket.Close()

	waitFor(t, func() bool { return p.conn.IsClosed() })

	if err = p.Publish(context.Background(), item); err != nil {
		t.Fatal("reconnect failed", err)
	}
	// Queue B keeps receiving while A's queue has no consumer.
	if _, ok, err := ch.Get(queues[1], true); err != nil || !ok {
		t.Fatal("subscriber B stalled", ok, err)
	}

	if _, ok, err := ch.Get(queues[0], true); err != nil || !ok {
		t.Fatal("subscriber A backlog absent", ok, err)
	}

	t.Log("verified publisher confirms, durable fanout, persistent messages, same IDs, offline queue backlog and connection recovery")
}
