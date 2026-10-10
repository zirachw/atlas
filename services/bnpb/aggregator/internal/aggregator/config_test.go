package aggregator

import (
	"context"
	"net"
	"testing"
	"time"
)

func validTestEnv(t *testing.T) {
	t.Helper()

	for key, value := range map[string]string{
		"DATABASE_URL":              "postgres://example:example@localhost/canonical",
		"RABBITMQ_URL":              "amqp://example:example@localhost/",
		"BMKG_BASE_URL":             "http://bmkg:8081",
		"PVMBG_BASE_URL":            "http://pvmbg:8082",
		"BMKG_API_KEY":              "test-bmkg",
		"PVMBG_TOKEN":               "test-pvmbg",
		"AGGREGATOR_INTERNAL_TOKEN": "test-internal",
	} {
		t.Setenv(key, value)
	}

	for _, key := range []string{
		"HTTP_ADDR",
		"POLL_INTERVAL",
		"SOURCE_TIMEOUT",
		"RETRY_BACKOFF",
		"QUERY_TIMEOUT",
		"PUBLISH_TIMEOUT",
		"SOURCE_RETRIES",
		"PUBLISH_RETRIES",
		"QUERY_CAPACITY",
		"PUBLISH_CAPACITY",
		"QUERY_POOL_SIZE",
		"INGEST_POOL_SIZE",
	} {
		t.Setenv(key, "")
	}
}

func TestConfigDefaultsAndRejection(t *testing.T) {
	validTestEnv(t)

	c, err := ConfigFromEnv()
	if err != nil || c.SourceTimeout != 4*time.Second || c.PollInterval != 3*time.Second || c.Address != ":8083" {
		t.Fatal(c, err)
	}

	for _, tc := range []struct{ key, value string }{
		{"BMKG_API_KEY", ""},
		{"PVMBG_TOKEN", "test-bmkg"},
		{"AGGREGATOR_INTERNAL_TOKEN", "test-pvmbg"},
		{"SOURCE_RETRIES", "6"},
		{"PUBLISH_RETRIES", "-1"},
		{"QUERY_CAPACITY", "0"},
		{"QUERY_POOL_SIZE", "0"},
		{"SOURCE_TIMEOUT", "0s"},
		{"POLL_INTERVAL", "bad"},
		{"BMKG_BASE_URL", "http://username:password@bmkg:8081"},
		{"PVMBG_BASE_URL", "http://pvmbg:8082?token=secret"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			validTestEnv(t)
			t.Setenv(tc.key, tc.value)

			if _, err := ConfigFromEnv(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestPublisherBoundsUnresponsiveBrokerHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	done := make(chan struct{})
	stop := make(chan struct{})

	go func() {
		defer close(done)

		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		<-stop
	}()

	p := &RabbitPublisher{URL: "amqp://test:test@" + listener.Addr().String() + "/", Timeout: 50 * time.Millisecond}
	defer p.Close()

	start := time.Now()
	err = p.Publish(context.Background(), Publication{Event: HazardEvent{HazardID: "bounded"}})

	close(stop)
	<-done

	if err == nil || time.Since(start) > time.Second {
		t.Fatal("unresponsive broker was not bounded", err)
	}
}

func TestConfigAllowsZeroRetriesButNotZeroCapacity(t *testing.T) {
	validTestEnv(t)
	t.Setenv("SOURCE_RETRIES", "0")
	t.Setenv("PUBLISH_RETRIES", "0")

	c, err := ConfigFromEnv()
	if err != nil || c.SourceRetries != 0 || c.PublishRetries != 0 {
		t.Fatal(c, err)
	}

	t.Setenv("PUBLISH_CAPACITY", "0")

	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("zero publish capacity accepted")
	}
}
