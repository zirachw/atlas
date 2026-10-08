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
	s, e := OpenStore(ctx, url, 2, 4)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.ingest.Exec(ctx, "TRUNCATE hazard_events; UPDATE source_progress SET checkpoint=NULL,last_success_at=NULL,stale_since=NULL"); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Close)
	return s
}
func TestIntegrationStoreCommitRollbackRestartAndOwnership(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	h, e := MapVolcanic(fixture(t, "volcanic-v1.json")[0], now)
	if e != nil {
		t.Fatal(e)
	}
	created, e := s.Save(ctx, "PVMBG", []HazardEvent{h}, &h.OccurredAt, now)
	if e != nil || len(created) != 1 {
		t.Fatal(created, e)
	}
	original := h.IngestedAt
	h.IngestedAt = now.Add(time.Hour)
	h.Attributes["confidence_level"] = json.RawMessage("0.8")
	created, e = s.Save(ctx, "PVMBG", []HazardEvent{h}, &h.OccurredAt, now)
	if e != nil || len(created) != 1 || created[0].Revision != 2 {
		t.Fatal(created, e)
	}
	h.Attributes["confidence_level"] = json.RawMessage(" 0.8 ")
	unchanged, err := s.Save(ctx, "PVMBG", []HazardEvent{h}, &h.OccurredAt, now)
	if err != nil || len(unchanged) != 0 {
		t.Fatal("unchanged JSON published", unchanged, err)
	}
	newer, e := MapVolcanic(fixture(t, "volcanic-v2.json")[0], now)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Save(ctx, "PVMBG", []HazardEvent{newer}, &newer.OccurredAt, now); e != nil {
		t.Fatal(e)
	}
	data, e := s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
	if e != nil || len(data.Data) != 2 {
		t.Fatal(data, e)
	}
	if data.Data[1].HazardID != h.HazardID || !data.Data[1].IngestedAt.Truncate(time.Microsecond).Equal(original.Truncate(time.Microsecond)) {
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
	if _, e = s.Save(ctx, "PVMBG", []HazardEvent{next, bad}, &now, now); e == nil {
		t.Fatal("invalid SQL constraint succeeded")
	}
	checkpoint, e := s.Checkpoint(ctx, "PVMBG")
	if e != nil || !checkpoint.Equal(newer.OccurredAt) {
		t.Fatal(checkpoint, e)
	}
	data, e = s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
	if e != nil || len(data.Data) != 2 {
		t.Fatal("rollback left row", data, e)
	}
	restarted, e := OpenStore(ctx, os.Getenv("TEST_DATABASE_URL"), 2, 4)
	if e != nil {
		t.Fatal(e)
	}
	defer restarted.Close()
	cp, e := restarted.Checkpoint(ctx, "PVMBG")
	if e != nil || !cp.Equal(*checkpoint) {
		t.Fatal("restart lost progress", cp, e)
	}
	data, e = restarted.Read(ctx, Query{Source: "PVMBG", Limit: 100})
	if e != nil || data.Sources[0].Status != "stale" || len(data.Data) != 2 {
		t.Fatal("restart freshness", data, e)
	}
	// The role used by Auth must not connect to Canonical Store.
	authURL := os.Getenv("TEST_AUTH_CANONICAL_URL")
	if authURL == "" {
		t.Fatal("missing ownership test URL")
	}
	conn, e := pgx.Connect(ctx, authURL)
	if e == nil {
		conn.Close(ctx)
		t.Fatal("Auth role gained Canonical Store access")
	}
	var pgErr *pgconn.PgError
	if !errors.As(e, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("expected permission denied, got %T %v", e, e)
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
	var schema, outage, delay, lateWarning atomic.Bool
	var bmkgCalls atomic.Int32
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
	if e := a.Poll(ctx, "seed-bmkg"); e != nil {
		t.Fatal(e)
	}
	if e := b.Poll(ctx, "seed-pvmbg"); e != nil {
		t.Fatal(e)
	}
	oldIDs := map[string]bool{}
	for len(pending) > 0 {
		oldIDs[(<-pending).Event.HazardID] = true
	}
	schema.Store(true)
	lateWarning.Store(true)
	if e := a.Poll(ctx, "late-warning"); e != nil {
		t.Fatal(e)
	}
	if e := b.Poll(ctx, "live-schema"); e != nil {
		t.Fatal(e)
	}
	if len(pending) != 2 {
		t.Fatal("missing late warning revision or new volcanic event", len(pending))
	}
	update := <-pending
	if !oldIDs[update.Event.HazardID] || update.Event.Revision != 2 || update.Event.Severity != "AWAS" {
		t.Fatal("late warning publication", update)
	}
	<-pending
	if e := a.Poll(ctx, "same-warning"); e != nil {
		t.Fatal(e)
	}
	if len(pending) != 0 {
		t.Fatal("unchanged warning republished")
	}
	data, e := s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
	if e != nil || len(data.Data) != 2 {
		t.Fatal(data, e)
	}
	if _, ok := data.Data[1].Attributes["confidence_level"]; ok {
		t.Fatal("old record rewritten with new shape")
	}
	if string(data.Data[0].Attributes["confidence_level"]) != "0.85" {
		t.Fatal("new field lost")
	}
	quakes, e := s.Read(ctx, Query{Source: "BMKG", Severity: "AWAS", Limit: 100})
	if e != nil || len(quakes.Data) != 1 || !oldIDs[quakes.Data[0].HazardID] {
		t.Fatal("late warning or identity failed", quakes, e)
	}
	api := (&API{s, "test-internal", time.Second, make(chan struct{}, 64), testLog()}).Handler()
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
	if e := a.Poll(ctx, "isolated-bmkg"); e != nil {
		t.Fatal(e)
	}
	if w := request(api, "/internal/hazards?source=BMKG", "test-internal"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if time.Since(start) > 300*time.Millisecond {
		t.Fatal("BMKG blocked on PVMBG")
	}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	delay.Store(false)
	outage.Store(true)
	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); b.Run(runCtx) }()
	waitFor(t, func() bool {
		r, e := s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
		return e == nil && r.Sources[0].Status == "stale" && r.Sources[0].StaleSince != nil
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
		r, e := s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
		return e == nil && r.Sources[0].Status == "healthy" && r.Sources[0].StaleSince == nil
	})
	cancel()
	wg.Wait()
	// Empty source during failure is explicitly unavailable, without generic 500.
	if _, e = s.ingest.Exec(ctx, "DELETE FROM hazard_events WHERE source='PVMBG'"); e != nil {
		t.Fatal(e)
	}
	if e = s.Failure(ctx, "PVMBG", time.Now()); e != nil {
		t.Fatal(e)
	}
	if request(api, "/internal/hazards?source=PVMBG", "test-internal").Code != 503 || request(api, "/internal/hazards", "test-internal").Code != 206 || request(api, "/internal/hazards?source=BMKG", "test-internal").Code != 200 {
		t.Fatal("unavailable source damaged mixed/seismic reads")
	}
	t.Log("verified live additive schema, late warning, three-second isolation, stored reads, stale outage and automatic recovery")
}
func TestIntegrationRabbitFanoutConfirmAndReconnect(t *testing.T) {
	broker := os.Getenv("TEST_RABBITMQ_URL")
	if broker == "" {
		t.Skip("set TEST_RABBITMQ_URL or run make integration")
	}
	conn, e := amqp.Dial(broker)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	ch, e := conn.Channel()
	if e != nil {
		t.Fatal(e)
	}
	defer ch.Close()
	if e = ch.ExchangeDeclare(Exchange, "fanout", true, false, false, false, nil); e != nil {
		t.Fatal(e)
	}
	queues := []string{"integration.a." + correlationID(), "integration.b." + correlationID()}
	for _, name := range queues {
		q, e := ch.QueueDeclare(name, true, false, false, false, amqp.Table{"x-max-length": int32(10000), "x-max-length-bytes": int32(100 << 20), "x-overflow": "reject-publish"})
		if e != nil {
			t.Fatal(e)
		}
		if e = ch.QueueBind(q.Name, "", Exchange, false, nil); e != nil {
			t.Fatal(e)
		}
		defer ch.QueueDelete(q.Name, false, false, false)
	}
	p := &RabbitPublisher{URL: broker, Timeout: time.Second}
	defer p.Close()
	h, e := MapVolcanic(fixture(t, "volcanic-v1.json")[0], time.Now())
	if e != nil {
		t.Fatal(e)
	}
	item := Publication{h, "broker-test"}
	if e = p.Publish(context.Background(), item); e != nil {
		t.Fatal(e)
	}
	for _, q := range queues {
		msg, ok, e := ch.Get(q, false)
		if e != nil || !ok {
			t.Fatal(q, ok, e)
		}
		var got HazardEvent
		if e = json.Unmarshal(msg.Body, &got); e != nil {
			t.Fatal(e)
		}
		if got.HazardID != h.HazardID || msg.MessageId != h.HazardID+":1" || msg.CorrelationId != "broker-test" || msg.DeliveryMode != amqp.Persistent {
			t.Fatal("message identity or metadata lost")
		}
		if e = msg.Ack(false); e != nil {
			t.Fatal(e)
		}
	}
	// Publish an updated snapshot with a distinct revision identity to both queues.
	item.Event.Revision = 2
	item.Event.Severity = "AWAS"
	if e = p.Publish(context.Background(), item); e != nil {
		t.Fatal(e)
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
		if got.HazardID != h.HazardID || got.Revision != 2 || got.Severity != "AWAS" || msg.MessageId != h.HazardID+":2" {
			t.Fatal("update metadata or payload lost", got)
		}
	}
	// Drop the actual network connection, then publish through a reconnected channel.
	_ = p.socket.Close()
	waitFor(t, func() bool { return p.conn.IsClosed() })
	if e = p.Publish(context.Background(), item); e != nil {
		t.Fatal("reconnect failed", e)
	}
	// Queue B keeps receiving while A's queue has no consumer.
	if _, ok, e := ch.Get(queues[1], true); e != nil || !ok {
		t.Fatal("subscriber B stalled", ok, e)
	}
	if _, ok, e := ch.Get(queues[0], true); e != nil || !ok {
		t.Fatal("subscriber A backlog absent", ok, e)
	}
	t.Log("verified publisher confirms, durable fanout, persistent messages, same IDs, offline queue backlog and connection recovery")
}
