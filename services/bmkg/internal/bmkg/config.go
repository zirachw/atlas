package bmkg

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	MinDelay         = 50 * time.Millisecond
	MaxDelay         = 150 * time.Millisecond
	MaxEventInterval = 10 * time.Second
)

type Config struct {
	Address       string
	DatabaseURL   string
	APIKey        string
	Delay         time.Duration
	EventInterval time.Duration
}

func ConfigFromEnv() (Config, error) {
	c := Config{Address: os.Getenv("HTTP_ADDR"), DatabaseURL: os.Getenv("DATABASE_URL"), APIKey: os.Getenv("BMKG_API_KEY")}
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL is required")
	}
	if c.Address == "" {
		c.Address = ":8081"
	}
	if c.APIKey == "" || strings.ContainsAny(c.APIKey, " \t\r\n") {
		return c, errors.New("BMKG_API_KEY is required and must not contain whitespace")
	}
	var err error
	if c.Delay, err = duration("RESPONSE_DELAY", 100*time.Millisecond, MinDelay, MaxDelay); err != nil {
		return c, err
	}
	if c.EventInterval, err = duration("EVENT_INTERVAL", MaxEventInterval, time.Second, MaxEventInterval); err != nil {
		return c, err
	}
	return c, nil
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
