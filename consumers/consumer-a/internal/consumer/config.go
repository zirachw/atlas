package consumer

import (
	"errors"
	"fmt"
	"os"
	"regexp"
)

type Config struct{ AMQPURL, Name, Queue, StatePath, HealthAddr string }

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
	return c, nil
}
