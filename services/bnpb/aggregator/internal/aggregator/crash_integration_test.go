package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// This test-only boundary terminates the process after the real SQL commit,
// before Poll can enqueue any created events. Production code has no fault hook.
type crashAfterCommitStore struct{ *Store }

func (s crashAfterCommitStore) Save(ctx context.Context, source string, events []HazardEvent, cp *time.Time, now time.Time) ([]HazardEvent, error) {
	created, err := s.Store.Save(ctx, source, events, cp, now)
	if err == nil && len(created) > 0 {
		os.Exit(77)
	}
	return created, err
}

func TestIntegrationSavePublishProcessCrash(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if os.Getenv("ATLAS_CRASH_CHILD") == "1" {
		s, err := OpenStore(ctx, os.Getenv("TEST_DATABASE_URL"), 2, 4)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		p := newPoller("PVMBG", os.Getenv("ATLAS_CRASH_SOURCE_URL"), crashAfterCommitStore{s}, make(chan Publication, 4))
		if err := p.Poll(ctx, "crash-after-commit"); err != nil {
			t.Fatal(err)
		}
		t.Fatal("fault boundary was not reached")
	}
	broker := os.Getenv("TEST_RABBITMQ_URL")
	if broker == "" {
		t.Skip("run make integration with PostgreSQL and RabbitMQ")
	}
	s := integrationStore(t)
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
	q, err := ch.QueueDeclare("integration.crash."+correlationID(), true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ch.QueueDelete(q.Name, false, false, false)
	if err = ch.QueueBind(q.Name, "", Exchange, false, nil); err != nil {
		t.Fatal(err)
	}
	original := fixture(t, "volcanic-v1.json")[0]
	next := fixture(t, "volcanic-v1.json")[0]
	next["report_id"] = json.RawMessage(`"after-crash-control"`)
	next["reported_at"] = json.RawMessage(`"2026-09-30T00:00:01Z"`)
	var recovered atomic.Bool
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-PVMBG" {
			w.WriteHeader(401)
			return
		}
		records := []Object{original}
		if recovered.Load() {
			records = append(records, next)
		}
		_ = json.NewEncoder(w).Encode(records)
	}))
	defer source.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(ctx, executable, "-test.run=^TestIntegrationSavePublishProcessCrash$")
	child.Env = append(os.Environ(), "ATLAS_CRASH_CHILD=1", "ATLAS_CRASH_SOURCE_URL="+source.URL)
	output, err := child.CombinedOutput()
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.ExitCode() != 77 {
		t.Fatalf("expected process exit 77: %v %s", err, output)
	}
	// Open fresh process state and verify the committed row and checkpoint survived.
	restarted, err := OpenStore(ctx, os.Getenv("TEST_DATABASE_URL"), 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	cp, err := restarted.Checkpoint(ctx, "PVMBG")
	expected, _ := timestamp(original, "reported_at")
	if err != nil || cp == nil || !cp.Equal(expected) {
		t.Fatal("commit checkpoint lost", cp, err)
	}
	data, err := s.Read(ctx, Query{Source: "PVMBG", Limit: 100})
	if err != nil || len(data.Data) != 1 {
		t.Fatal("committed row lost", data, err)
	}
	lostID := data.Data[0].HazardID
	pending := make(chan Publication, 4)
	p := newPoller("PVMBG", source.URL, restarted, pending)
	if err = p.Poll(ctx, "restart-overlap"); err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatal("unexpected replay of committed event")
	}
	if _, ok, err := ch.Get(q.Name, true); err != nil || ok {
		t.Fatal("unexpected publication of crash event", ok, err)
	}
	// A subsequent new event does publish: recovery works, but the old gap remains.
	recovered.Store(true)
	if err = p.Poll(ctx, "post-crash-control"); err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatal("new event not queued", len(pending))
	}
	item := <-pending
	if item.Event.HazardID == lostID {
		t.Fatal("control reused crash event")
	}
	publisher := &RabbitPublisher{URL: broker, Timeout: time.Second}
	defer publisher.Close()
	if err = publisher.Publish(ctx, item); err != nil {
		t.Fatal(err)
	}
	msg, ok, err := ch.Get(q.Name, true)
	expectedID := fmt.Sprintf("%s:%d", item.Event.HazardID, item.Event.Revision)
	if err != nil || !ok || msg.MessageId != expectedID {
		t.Fatal("new event did not reach broker", ok, err)
	}
	var published HazardEvent
	if err := json.Unmarshal(msg.Body, &published); err != nil {
		t.Fatal(err)
	}
	if published.HazardID != item.Event.HazardID || published.Revision != item.Event.Revision {
		t.Fatal("control event identity or revision changed", published)
	}
	if _, ok, err = ch.Get(q.Name, true); err != nil || ok {
		t.Fatal("unexpected replay after recovery", ok, err)
	}
	t.Logf("process_exit=77 committed_hazard_id=%s checkpoint=%s replay_publications=0 control_hazard_id=%s confirmed=true; known M1 save/publish loss reproduced", lostID, cp.Format(time.RFC3339), item.Event.HazardID)
}
