package demo

import (
	"testing"
	"time"
)

func TestLongestConnectionSpan(t *testing.T) {
	base := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	regular := func(count int, connections func(int) int) []connectionSample {
		out := make([]connectionSample, count)
		for i := range count {
			out[i] = connectionSample{base.Add(time.Duration(i) * 2 * time.Second), connections(i)}
		}
		return out
	}
	for _, tc := range []struct {
		name    string
		samples []connectionSample
		want    time.Duration
	}{
		{"empty", nil, 0},
		{"single observation", []connectionSample{{base, 50}}, 0},
		{"thirty samples span only 58 seconds", regular(30, func(int) int { return 50 }), 58 * time.Second},
		{"continuous sixty seconds", regular(31, func(int) int { return 50 }), time.Minute},
		{"scattered qualifying samples", regular(45, func(i int) int {
			if i%3 == 2 {
				return 20
			}
			return 60
		}), 2 * time.Second},
		{"low count resets interval", []connectionSample{{base, 50}, {base.Add(2 * time.Second), 49}, {base.Add(4 * time.Second), 50}, {base.Add(7 * time.Second), 60}}, 3 * time.Second},
		{"long observation gap resets interval", []connectionSample{{base, 60}, {base.Add(time.Minute), 60}, {base.Add(62 * time.Second), 60}}, 2 * time.Second},
		{"backwards clock resets interval", []connectionSample{{base, 60}, {base.Add(-time.Second), 60}, {base.Add(time.Second), 60}}, 2 * time.Second},
		{"elapsed time rather than sample count", []connectionSample{{base, 60}, {base.Add(3 * time.Second), 60}, {base.Add(7 * time.Second), 60}}, 7 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := longestConnectionSpan(tc.samples, 50, 5*time.Second); got != tc.want {
				t.Fatalf("span %s, want %s", got, tc.want)
			}
		})
	}
}
