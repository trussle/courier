package queue

import (
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/trussle/courier/pkg/models"
)

// Queue represents a series of records
// The queue's underlying backing store is a constructed from a channel, so it
// blocks if no body dequeues any items.
type Queue interface {
	// Enqueue a record
	Enqueue(models.Record) error

	// Dequeue a record from the channel
	Dequeue() ([]models.Record, error)

	// Commit a transaction containing the records, so that an ack can be sent
	Commit(models.Transaction) (Result, error)

	// Failed a transaction containing the records, so that potential retries can
	// be used.
	Failed(models.Transaction) (Result, error)
}

// Result returns the amount of successes and failures
type Result struct {
	Success, Failure int
}

// Config encapsulates the requirements for generating a Queue
type Config struct {
	name         string
	remoteConfig *RemoteConfig
}

// Option defines a option for generating a queue Config
type Option func(*Config) error

// Build ingests configuration options to then yield a Config and return an
// error if it fails during setup.
func Build(opts ...Option) (*Config, error) {
	var config Config
	for _, opt := range opts {
		err := opt(&config)
		if err != nil {
			return nil, err
		}
	}
	return &config, nil
}

// With adds a type of queue to use for the configuration.
func With(name string) Option {
	return func(config *Config) error {
		config.name = name
		return nil
	}
}

// WithConfig adds a remote queue config to the configuration
func WithConfig(remoteConfig *RemoteConfig) Option {
	return func(config *Config) error {
		config.remoteConfig = remoteConfig
		return nil
	}
}

// New creates a queue from a configuration or returns error if on failure.
func New(config *Config, logger log.Logger) (queue Queue, err error) {
	switch strings.ToLower(config.name) {
	case "remote":
		queue, err = newRemoteQueue(config.remoteConfig, logger)
		if err != nil {
			err = errors.Wrap(err, "remote queue")
			return
		}
	case "virtual":
		queue = newVirtualQueue()
	case "nop":
		queue = newNopQueue()
	default:
		err = errors.Errorf("unexpected fs type %q", config.name)
	}
	return
}
