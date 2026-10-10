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

const (
	defaultName           = "consumer-a"
	defaultQueue          = "hazard.events.consumer-a"
	defaultStatePath      = "/data/processed-hazard-ids.txt"
	defaultHealthAddr     = ":8091"
	defaultPrefetch       = 1
	defaultQueueMaxLength = 10000
	defaultQueueMaxBytes  = 64 << 20
	defaultMessageTTL     = time.Hour
)

var queuePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func (c Config) queueArguments() amqp.Table {
	return amqp.Table{
		"x-max-length":       c.QueueMaxLength,
		"x-max-length-bytes": c.QueueMaxBytes,
		"x-message-ttl":      c.MessageTTL.Milliseconds(),
		"x-overflow":         "reject-publish",
	}
}

func ConfigFromEnv() (Config, error) {
	c := Config{
		AMQPURL:    os.Getenv("AMQP_URL"),
		Name:       os.Getenv("CONSUMER_NAME"),
		Queue:      os.Getenv("CONSUMER_QUEUE"),
		StatePath:  os.Getenv("CONSUMER_STATE_PATH"),
		HealthAddr: os.Getenv("CONSUMER_HEALTH_ADDR"),
	}
	// Set first, so even a configuration error is logged under the right service name.
	if c.Name == "" {
		c.Name = defaultName
	}

	if c.AMQPURL == "" {
		return c, errors.New("AMQP_URL is required")
	}

	if c.Queue == "" {
		c.Queue = defaultQueue
	}

	if c.StatePath == "" {
		c.StatePath = defaultStatePath
	}

	if c.HealthAddr == "" {
		c.HealthAddr = defaultHealthAddr
	}

	if !queuePattern.MatchString(c.Name) || !queuePattern.MatchString(c.Queue) {
		return c, fmt.Errorf("invalid consumer name or queue")
	}

	c.Prefetch = defaultPrefetch
	c.QueueMaxLength, c.QueueMaxBytes, c.MessageTTL = defaultQueueMaxLength, defaultQueueMaxBytes, defaultMessageTTL

	if raw := os.Getenv("CONSUMER_PREFETCH"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 1024 {
			return c, errors.New("invalid CONSUMER_PREFETCH")
		}

		c.Prefetch = value
	}

	limits := map[string]*int64{
		"CONSUMER_QUEUE_MAX_LENGTH": &c.QueueMaxLength,
		"CONSUMER_QUEUE_MAX_BYTES":  &c.QueueMaxBytes,
	}
	for name, target := range limits {
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
