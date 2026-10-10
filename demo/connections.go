package demo

import "time"

type connectionSample struct {
	At          time.Time `json:"at"`
	Connections int       `json:"connections"`
}

func longestConnectionSpan(samples []connectionSample, minimum int, maximumGap time.Duration) time.Duration {
	var (
		start, previous time.Time
		longest         time.Duration
	)

	for _, sample := range samples {
		gap := sample.At.Sub(previous)
		if sample.Connections < minimum {
			start = time.Time{}
		} else {
			if start.IsZero() || gap <= 0 || gap > maximumGap {
				start = sample.At
			}

			longest = max(longest, sample.At.Sub(start))
		}

		previous = sample.At
	}

	return longest
}
