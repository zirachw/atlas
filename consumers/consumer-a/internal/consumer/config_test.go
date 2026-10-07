package consumer

import (
	"testing"
	"time"
)

func TestQueueConfiguration(t *testing.T) {
	t.Setenv("AMQP_URL", "amqp://example:example@localhost:5672/")
	for _, name := range []string{"CONSUMER_PREFETCH", "CONSUMER_QUEUE_MAX_LENGTH", "CONSUMER_QUEUE_MAX_BYTES", "CONSUMER_MESSAGE_TTL"} {
		t.Setenv(name, "")
	}
	c, err := ConfigFromEnv()
	if err != nil || c.Prefetch != 1 || c.QueueMaxLength != 10000 || c.QueueMaxBytes != 64<<20 || c.MessageTTL != time.Hour {
		t.Fatalf("defaults=%+v err=%v", c, err)
	}
	t.Setenv("CONSUMER_PREFETCH", "4")
	t.Setenv("CONSUMER_QUEUE_MAX_LENGTH", "120")
	t.Setenv("CONSUMER_QUEUE_MAX_BYTES", "4096")
	t.Setenv("CONSUMER_MESSAGE_TTL", "15m")
	c, err = ConfigFromEnv()
	if err != nil || c.Prefetch != 4 || c.queueArguments()["x-max-length"] != int64(120) || c.queueArguments()["x-message-ttl"] != int64(900000) || c.queueArguments()["x-overflow"] != "reject-publish" {
		t.Fatalf("configured=%+v err=%v", c, err)
	}
	for _, name := range []string{"CONSUMER_PREFETCH", "CONSUMER_QUEUE_MAX_LENGTH", "CONSUMER_QUEUE_MAX_BYTES", "CONSUMER_MESSAGE_TTL"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "0")
			if _, err := ConfigFromEnv(); err == nil {
				t.Fatal("zero limit accepted")
			}
		})
	}
}
