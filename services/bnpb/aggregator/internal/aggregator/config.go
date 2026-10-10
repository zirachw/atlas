package aggregator

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL, RabbitMQURL, BMKGURL, PVMBGURL, BMKGKey, PVMBGToken, InternalToken, Address string
	PollInterval, SourceTimeout, Backoff, QueryTimeout, PublishTimeout                       time.Duration
	SourceRetries, PublishRetries, QueryCapacity, PublishCapacity                            int
	IngestPool, QueryPool                                                                    int32
}

const (
	defaultAddress         = ":8083"
	defaultPollInterval    = 3 * time.Second
	defaultSourceTimeout   = 4 * time.Second
	defaultRetryBackoff    = 200 * time.Millisecond
	defaultQueryTimeout    = 2 * time.Second
	defaultPublishTimeout  = 3 * time.Second
	defaultSourceRetries   = 2
	defaultPublishRetries  = 2
	defaultQueryCapacity   = 64
	defaultPublishCapacity = 256
	defaultIngestPool      = 2
	defaultQueryPool       = 8
)

func ConfigFromEnv() (Config, error) {
	c := Config{
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		RabbitMQURL:   os.Getenv("RABBITMQ_URL"),
		BMKGURL:       os.Getenv("BMKG_BASE_URL"),
		PVMBGURL:      os.Getenv("PVMBG_BASE_URL"),
		BMKGKey:       os.Getenv("BMKG_API_KEY"),
		PVMBGToken:    os.Getenv("PVMBG_TOKEN"),
		InternalToken: os.Getenv("AGGREGATOR_INTERNAL_TOKEN"),
		Address:       os.Getenv("HTTP_ADDR"),
	}
	for _, v := range []string{
		c.DatabaseURL,
		c.RabbitMQURL,
		c.BMKGURL,
		c.PVMBGURL,
		c.BMKGKey,
		c.PVMBGToken,
		c.InternalToken,
	} {
		if v == "" {
			return c, errors.New("required environment configuration missing")
		}
	}

	for _, raw := range []string{c.BMKGURL, c.PVMBGURL} {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil ||
			u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
			return c, errors.New("source URL must be an HTTP origin without credentials")
		}
	}

	if c.BMKGKey == c.PVMBGToken || c.InternalToken == c.BMKGKey || c.InternalToken == c.PVMBGToken {
		return c, errors.New("credentials must be independent")
	}

	if c.Address == "" {
		c.Address = defaultAddress
	}

	var (
		err                   error
		ingestPool, queryPool int
	)

	durations := []struct {
		key      string
		fallback time.Duration
		dest     *time.Duration
	}{
		{"POLL_INTERVAL", defaultPollInterval, &c.PollInterval},
		{"SOURCE_TIMEOUT", defaultSourceTimeout, &c.SourceTimeout},
		{"RETRY_BACKOFF", defaultRetryBackoff, &c.Backoff},
		{"QUERY_TIMEOUT", defaultQueryTimeout, &c.QueryTimeout},
		{"PUBLISH_TIMEOUT", defaultPublishTimeout, &c.PublishTimeout},
	}
	for _, d := range durations {
		if *d.dest, err = envDuration(d.key, d.fallback); err != nil {
			return c, err
		}
	}

	ints := []struct {
		key                string
		fallback, min, max int
		dest               *int
	}{
		{"SOURCE_RETRIES", defaultSourceRetries, 0, 5, &c.SourceRetries},
		{"PUBLISH_RETRIES", defaultPublishRetries, 0, 5, &c.PublishRetries},
		{"QUERY_CAPACITY", defaultQueryCapacity, 1, 1024, &c.QueryCapacity},
		{"PUBLISH_CAPACITY", defaultPublishCapacity, 1, 10000, &c.PublishCapacity},
		{"INGEST_POOL_SIZE", defaultIngestPool, 1, 64, &ingestPool},
		{"QUERY_POOL_SIZE", defaultQueryPool, 1, 64, &queryPool},
	}
	for _, d := range ints {
		if *d.dest, err = envInt(d.key, d.fallback, d.min, d.max); err != nil {
			return c, err
		}
	}

	c.IngestPool, c.QueryPool = int32(ingestPool), int32(queryPool)

	return c, nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 || d > time.Minute {
		return 0, fmt.Errorf("invalid %s", key)
	}

	return d, nil
}

func envInt(key string, fallback, min, max int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("invalid %s", key)
	}

	return n, nil
}
