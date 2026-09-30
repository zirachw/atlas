package aggregator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type publisherStub struct {
	items []Publication
	fail  int
	mu    sync.Mutex
}

func (p *publisherStub) Publish(ctx context.Context, item Publication) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.items = append(p.items, item)
	if len(p.items) <= p.fail {
		return errors.New("unconfirmed")
	}
	return nil
}
func (p *publisherStub) Close() {}
func TestPublisherRetryKeepsIdentityAndBounds(t *testing.T) {
	item := Publication{Event: HazardEvent{HazardID: "stable-id"}, CorrelationID: "poll-id"}
	for _, tc := range []struct {
		fail, attempts int
		success        bool
	}{{1, 2, true}, {10, 3, false}} {
		p := &publisherStub{fail: tc.fail}
		w := &PublishWorker{Publisher: p, Timeout: time.Second, Backoff: time.Millisecond, Retries: 2, Log: testLog()}
		if w.send(context.Background(), item) != tc.success || len(p.items) != tc.attempts {
			t.Fatal(p.items)
		}
		for _, got := range p.items {
			if got.Event.HazardID != item.Event.HazardID || got.CorrelationID != item.CorrelationID {
				t.Fatal("retry identity changed")
			}
		}
	}
}
func TestPublicationQueueOverflowDoesNotStopCommit(t *testing.T) {
	// Queue failure remains an explicit documented loss after persistence.
	store := newMemoryStore()
	server := sourceFixtureServer(t, "volcanic-v2.json")
	defer server.Close()
	pending := make(chan Publication, 1)
	pending <- Publication{Event: HazardEvent{HazardID: "occupied"}}
	p := newPoller("PVMBG", server.URL, store, pending)
	if e := p.Poll(context.Background(), "overflow"); e != nil {
		t.Fatal(e)
	}
	if len(store.events) != 1 || len(pending) != 1 {
		t.Fatal("overflow blocked ingestion")
	}
	// A committed event omitted from publication is not recreated by duplicate polling.
	<-pending
	if e := p.Poll(context.Background(), "after-gap"); e != nil {
		t.Fatal(e)
	}
	if len(pending) != 0 {
		t.Fatal("unexpected outbox guarantee")
	}
}

func sourceFixtureServer(t *testing.T, name string) *httptest.Server {
	data := fixture(t, name)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(data) }))
}
