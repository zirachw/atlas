//go:build demo

package demo

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

func confirmed(t *testing.T, since time.Time) map[string]bool {
	t.Helper()

	ids := map[string]bool{}
	for _, l := range s.logged(
		t,
		"aggregator",
		since,
		func(l map[string]any) bool { return l["msg"] == "broker_call" && l["confirmed"] == true },
	) {
		ids[fmt.Sprint(l["hazard_id"])] = true
	}

	return ids
}

func checkFanout(t *testing.T, ev map[string]any) {
	s.ready(t)

	since := time.Now().Add(-2 * time.Second)

	var shared string

	eventually(t, 60*time.Second, "one confirmed event processed by both consumers", func() bool {
		a, b := s.processed(t, "consumer-a", since), s.processed(t, "consumer-b", since)
		for id := range confirmed(t, since) {
			if a[id] && b[id] {
				shared = id
				return true
			}
		}

		return false
	})

	ev["hazard_id"] = shared
	ev["path"] = "aggregator broker_call confirmed, then event_processed in consumer-a and consumer-b"
}

func checkConsumerOutage(t *testing.T, ev map[string]any) {
	s.ready(t)
	s.mustCompose(t, "stop", "consumer-a")

	down := time.Now().Add(-time.Second)

	var missed []string

	eventually(t, 60*time.Second, "producer and consumer B progressing while A is down", func() bool {
		missed = missed[:0]

		b := s.processed(t, "consumer-b", down)
		for id := range confirmed(t, down) {
			if b[id] {
				missed = append(missed, id)
			}
		}

		return len(missed) >= 2
	})

	stopped := time.Since(down)

	s.mustCompose(t, "start", "consumer-a")

	restarted := time.Now()

	eventually(t, 60*time.Second, "consumer A catching up on missed events", func() bool {
		a := s.processed(t, "consumer-a", down)
		return !slices.ContainsFunc(missed, func(id string) bool { return !a[id] })
	})

	ev["missed_while_down"] = missed
	ev["seconds_down"] = stopped.Seconds()
	ev["catch_up_seconds"] = time.Since(restarted).Seconds()
}

func checkNewConsumer(t *testing.T, ev map[string]any) {
	s.ready(t)
	aggregator := s.containerID(t, "aggregator")
	since := time.Now().Add(-time.Second)

	s.mustCompose(
		t,
		"--profile",
		"consumer-c-demo",
		"up",
		"-d",
		"--build",
		"--wait",
		"--wait-timeout",
		"120",
		"consumer-c-demo",
	)

	var shared string

	eventually(t, 60*time.Second, "the new consumer processing a new event", func() bool {
		b, c := s.processed(t, "consumer-b", since), s.processed(t, "consumer-c-demo", since)
		for id := range c {
			if b[id] {
				shared = id
				return true
			}
		}

		return false
	})

	if s.containerID(t, "aggregator") != aggregator {
		t.Error("the producer restarted when the consumer joined")
	}

	ev["hazard_id"] = shared
	ev["producer_container_unchanged"] = aggregator[:12]
}
