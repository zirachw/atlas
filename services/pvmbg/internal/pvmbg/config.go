package pvmbg

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"regexp"
	"time"
)

const (
	MinDelay         = 500 * time.Millisecond
	MaxDelay         = 3 * time.Second
	MaxEventInterval = 10 * time.Second
)

// Matches pvmbg.<key-id>.<secret>, so BMKG's plain key never matches.
var tokenFormat = regexp.MustCompile(`^pvmbg\.[a-z0-9-]{1,32}\.[A-Za-z0-9_-]{16,128}$`)

type Config struct {
	Address            string
	DatabaseURL        string
	Token              string
	MinDelay, MaxDelay time.Duration
	EventInterval      time.Duration
}

func ConfigFromEnv() (Config, error) {
	c := Config{Address: os.Getenv("HTTP_ADDR"), DatabaseURL: os.Getenv("DATABASE_URL"), Token: os.Getenv("PVMBG_TOKEN")}
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL is required")
	}
	if c.Address == "" {
		c.Address = ":8082"
	}
	if !tokenFormat.MatchString(c.Token) {
		return c, errors.New("PVMBG_TOKEN must have the form pvmbg.<key-id>.<secret> with a secret of at least 16 URL-safe characters")
	}
	var err error
	if c.MinDelay, err = duration("MIN_RESPONSE_DELAY", MinDelay, MinDelay, MaxDelay); err != nil {
		return c, err
	}
	if c.MaxDelay, err = duration("MAX_RESPONSE_DELAY", MaxDelay, MinDelay, MaxDelay); err != nil {
		return c, err
	}
	if c.MinDelay > c.MaxDelay {
		return c, errors.New("MIN_RESPONSE_DELAY must not exceed MAX_RESPONSE_DELAY")
	}
	if c.EventInterval, err = duration("EVENT_INTERVAL", MaxEventInterval, time.Second, MaxEventInterval); err != nil {
		return c, err
	}
	return c, nil
}

// Uniform in [MinDelay, MaxDelay], so equal bounds give a fixed delay.
func (c Config) RandomDelay() func() time.Duration {
	return func() time.Duration { return c.MinDelay + rand.N(c.MaxDelay-c.MinDelay+1) }
}

func duration(key string, fallback, min, max time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < min || d > max {
		return 0, fmt.Errorf("%s must be a duration between %s and %s", key, min, max)
	}
	return d, nil
}
