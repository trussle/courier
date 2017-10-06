package stream

import (
	"time"

	"github.com/trussle/courier/pkg/fs"
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/uuid"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
)

// Stream defines a queue of segments that are to be replayed on.
type Stream interface {

	// Append a Segment to the log, if it fails then it will return an error
	Append(queue.Segment) error

	// Walk over each record in every segment sequentially.
	Walk(func(queue.Segment) error) error

	// Commit transacts all the segments.
	Commit(*Transaction) error

	// Failed terminates all the segments.
	Failed(*Transaction) error

	// Len returns all the length of what's to be read
	Len() int

	// Capacity returns if the log is at capacity
	// More items can be filled in, nothing will be rejected, it's just an
	// indicator that the log is full.
	Capacity() bool

	// Reset completely resets the log back to a fresh state.
	Reset() error
}

// All returns a transaction that states everything should be commited
func All() *Transaction {
	return &Transaction{
		wildcard: true,
		segments: make(map[uuid.UUID][]uuid.UUID),
	}
}

// Transaction holds a list of segment ids and associated record ids, useful
// when commiting or failling a series of records.
type Transaction struct {
	wildcard bool
	segments map[uuid.UUID][]uuid.UUID
	size     int
}

// NewTransaction creates a new transaction
func NewTransaction() *Transaction {
	return &Transaction{
		wildcard: false,
		segments: make(map[uuid.UUID][]uuid.UUID),
		size:     0,
	}
}

// Set adds a segment id and associated records ids
func (t *Transaction) Set(id uuid.UUID, ids []uuid.UUID) {
	t.segments[id] = ids
	t.size += len(ids)
}

// Get selects a segment id to retirevie the associated records ids
func (t *Transaction) Get(id uuid.UUID) ([]uuid.UUID, bool) {
	ids, ok := t.segments[id]
	return ids, ok
}

// Len returns the transaction size
func (t *Transaction) Len() int {
	return t.size
}

// All returns if everything should be used
func (t *Transaction) All() bool {
	return t.wildcard
}

// Config encapsulates the requirements for generating a Stream
type Config struct {
	name string
	fsys fs.Filesystem
	root string
	size int
	age  time.Duration
}

// Option defines a option for generating a stream Config
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

// With adds a type of stream to use for the configuration.
func With(name string) Option {
	return func(config *Config) error {
		config.name = name
		return nil
	}
}

// WithFilesystem adds a type of stream to use for the configuration.
func WithFilesystem(fsys fs.Filesystem) Option {
	return func(config *Config) error {
		config.fsys = fsys
		return nil
	}
}

// WithRootDir adds a type of stream to use for the configuration.
func WithRootDir(root string) Option {
	return func(config *Config) error {
		config.root = root
		return nil
	}
}

// WithTargetSize adds a type of stream to use for the configuration.
func WithTargetSize(size int) Option {
	return func(config *Config) error {
		config.size = size
		return nil
	}
}

// WithTargetAge adds a type of stream to use for the configuration.
func WithTargetAge(age time.Duration) Option {
	return func(config *Config) error {
		config.age = age
		return nil
	}
}

// New returns a new stream
func New(config *Config, logger log.Logger) (stream Stream, err error) {
	switch config.name {
	case "local":
		stream, err = newLocalStream(config.fsys, config.root, config.size, config.age)
	case "virtual":
		stream = newVirtualStream(config.size, config.age)
	default:
		err = errors.Errorf("unexpected queue type %q", config.name)
	}
	return
}
