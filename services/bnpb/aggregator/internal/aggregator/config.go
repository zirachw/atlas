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

func ConfigFromEnv() (Config, error) {
	c := Config{DatabaseURL: os.Getenv("DATABASE_URL"), RabbitMQURL: os.Getenv("RABBITMQ_URL"), BMKGURL: os.Getenv("BMKG_BASE_URL"), PVMBGURL: os.Getenv("PVMBG_BASE_URL"), BMKGKey: os.Getenv("BMKG_API_KEY"), PVMBGToken: os.Getenv("PVMBG_TOKEN"), InternalToken: os.Getenv("AGGREGATOR_INTERNAL_TOKEN"), Address: os.Getenv("HTTP_ADDR")}
	for _, v := range []string{c.DatabaseURL, c.RabbitMQURL, c.BMKGURL, c.PVMBGURL, c.BMKGKey, c.PVMBGToken, c.InternalToken} {
		if v == "" {
			return c, errors.New("required environment configuration missing")
		}
	}
	for _, raw := range []string{c.BMKGURL, c.PVMBGURL} {
		u, e := url.Parse(raw)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
			return c, errors.New("source URL must be an HTTP origin without credentials")
		}
	}
	if c.BMKGKey == c.PVMBGToken || c.InternalToken == c.BMKGKey || c.InternalToken == c.PVMBGToken {
		return c, errors.New("credentials must be independent")
	}
	if c.Address == "" {
		c.Address = ":8083"
	}
	durations := []struct {
		key      string
		fallback time.Duration
		dest     *time.Duration
	}{
		{"POLL_INTERVAL", 3 * time.Second, &c.PollInterval}, {"SOURCE_TIMEOUT", 4 * time.Second, &c.SourceTimeout}, {"RETRY_BACKOFF", 200 * time.Millisecond, &c.Backoff}, {"QUERY_TIMEOUT", 2 * time.Second, &c.QueryTimeout}, {"PUBLISH_TIMEOUT", 3 * time.Second, &c.PublishTimeout},
	}
	for _, d := range durations {
		v := d.fallback
		if raw := os.Getenv(d.key); raw != "" {
			n, e := time.ParseDuration(raw)
			if e != nil || n <= 0 || n > time.Minute {
				return c, fmt.Errorf("invalid %s", d.key)
			}
			v = n
		}
		*d.dest = v
	}
	ints := []struct {
		key           string
		fallback, max int
		dest          *int
	}{{"SOURCE_RETRIES", 2, 5, &c.SourceRetries}, {"PUBLISH_RETRIES", 2, 5, &c.PublishRetries}, {"QUERY_CAPACITY", 64, 1024, &c.QueryCapacity}, {"PUBLISH_CAPACITY", 256, 10000, &c.PublishCapacity}}
	for _, d := range ints {
		v := d.fallback
		if raw := os.Getenv(d.key); raw != "" {
			n, e := strconv.Atoi(raw)
			if e != nil || n < 0 || n > d.max || n == 0 && d.max > 5 {
				return c, fmt.Errorf("invalid %s", d.key)
			}
			v = n
		}
		*d.dest = v
	}
	for _, d := range []struct {
		key      string
		fallback int32
		dest     *int32
	}{{"INGEST_POOL_SIZE", 2, &c.IngestPool}, {"QUERY_POOL_SIZE", 8, &c.QueryPool}} {
		v := d.fallback
		if raw := os.Getenv(d.key); raw != "" {
			n, e := strconv.Atoi(raw)
			if e != nil || n < 1 || n > 64 {
				return c, fmt.Errorf("invalid %s", d.key)
			}
			v = int32(n)
		}
		*d.dest = v
	}
	return c, nil
}
