package consumer

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	AMQPURL, Name, Queue, StatePath, HealthAddr string
	Prefetch                                    int
	QueueMaxLength, QueueMaxBytes               int64
	MessageTTL                                  time.Duration
}

func (c Config) queueArguments() amqp.Table {
	length, bytes, ttl := c.QueueMaxLength, c.QueueMaxBytes, c.MessageTTL
	if length == 0 {
		length = 10000
	}
	if bytes == 0 {
		bytes = 64 << 20
	}
	if ttl == 0 {
		ttl = time.Hour
	}
	return amqp.Table{"x-max-length": length, "x-max-length-bytes": bytes, "x-message-ttl": ttl.Milliseconds(), "x-overflow": "reject-publish"}
}

var queuePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func ConfigFromEnv() (Config, error) {
	c := Config{AMQPURL: os.Getenv("AMQP_URL"), Name: os.Getenv("CONSUMER_NAME"), Queue: os.Getenv("CONSUMER_QUEUE"), StatePath: os.Getenv("CONSUMER_STATE_PATH"), HealthAddr: os.Getenv("CONSUMER_HEALTH_ADDR")}
	if c.AMQPURL == "" {
		return c, errors.New("AMQP_URL is required")
	}
	if c.Name == "" {
		c.Name = "consumer-a"
	}
	if c.Queue == "" {
		c.Queue = "hazard.events.consumer-a"
	}
	if c.StatePath == "" {
		c.StatePath = "/data/processed-hazard-ids.txt"
	}
	if c.HealthAddr == "" {
		c.HealthAddr = ":8091"
	}
	if !queuePattern.MatchString(c.Name) || !queuePattern.MatchString(c.Queue) {
		return c, fmt.Errorf("invalid consumer name or queue")
	}
	c.Prefetch = 1
	c.QueueMaxLength, c.QueueMaxBytes, c.MessageTTL = 10000, 64<<20, time.Hour
	if raw := os.Getenv("CONSUMER_PREFETCH"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 1024 {
			return c, errors.New("invalid CONSUMER_PREFETCH")
		}
		c.Prefetch = value
	}
	for name, target := range map[string]*int64{"CONSUMER_QUEUE_MAX_LENGTH": &c.QueueMaxLength, "CONSUMER_QUEUE_MAX_BYTES": &c.QueueMaxBytes} {
		if raw := os.Getenv(name); raw != "" {
			value, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || value < 1 {
				return c, fmt.Errorf("invalid %s", name)
			}
			*target = value
		}
	}
	if raw := os.Getenv("CONSUMER_MESSAGE_TTL"); raw != "" {
		value, err := time.ParseDuration(raw)
		if err != nil || value < time.Second || value > 7*24*time.Hour {
			return c, errors.New("invalid CONSUMER_MESSAGE_TTL")
		}
		c.MessageTTL = value
	}
	return c, nil
}
