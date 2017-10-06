package queue

import (
	"strings"

	"github.com/trussle/courier/pkg/uuid"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
)

// Queue is an abstraction for segments on an ingest node.
type Queue interface {

	// Enqueue returns a new segment that can be written to.
	Enqueue(Record) error

	// Dequeue returns the first segment, which can then be read from.
	Dequeue() (Segment, error)

	// Reset clears the queue, note all information will be lost. If it can't
	// clear the queue or there is an error performing reset then an error will
	// be returned.
	Reset() error
}

// Segment is a segment that can be read from. Once read, it may be
// committed and thus deleted. Or it may be failed, and made available for
// selection again.
type Segment interface {

	// Unique ID of the segment
	ID() uuid.UUID

	// Walk over the records in the segment.
	Walk(func(Record) error) error

	// Commit attempts to to commit a read segment or fails on error
	Commit([]uuid.UUID) (Result, error)

	// Failed notifies the read segment or fails with an error
	Failed([]uuid.UUID) (Result, error)

	// Size gets the size of the read segment.
	Size() int
}

// Result represents how many items where successful, unsuccessful when
// transacting
type Result struct {
	Successful, Unsuccessful int
}

type noSegmentsAvailable interface {
	NoSegmentsAvailable() bool
}

type errNoSegmentsAvailable struct {
	err error
}

func (e errNoSegmentsAvailable) Error() string {
	return e.err.Error()
}

func (e errNoSegmentsAvailable) NoSegmentsAvailable() bool {
	return true
}

// ErrNoSegmentsAvailable tests to see if the error passed if no segments are
// available
func ErrNoSegmentsAvailable(err error) bool {
	if err != nil {
		if _, ok := err.(noSegmentsAvailable); ok {
			return true
		}
	}
	return false
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
		queue, err = NewRemoteQueue(config.remoteConfig, logger)
		if err != nil {
			err = errors.Wrap(err, "remote queue")
			return
		}
	case "virtual":
		queue = NewVirtualQueue()
	case "nop":
		queue = NewNopQueue()
	default:
		err = errors.Errorf("unexpected fs type %q", config.name)
	}
	return
}
