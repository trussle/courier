package store

import (
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
)

// Store holds identifiers with associated records
type Store interface {

	// Add a transaction of identifiers to a associated to the store.
	Add([]string) error

	// Intersection reports back the union and difference of the identifiers
	// found with in the store.
	Intersection([]string) (union, difference []string, err error)
}

// Config encapsulates the requirements for generating a Stream
type Config struct {
	name         string
	size         int
	remoteConfig *RemoteConfig
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

// WithSize adds a size cap to the configuration.
func WithSize(size int) Option {
	return func(config *Config) error {
		config.size = size
		return nil
	}
}

// WithRemoteConfig adds a remote log config to the configuration
func WithRemoteConfig(remoteConfig *RemoteConfig) Option {
	return func(config *Config) error {
		config.remoteConfig = remoteConfig
		return nil
	}
}

// New returns a new log
func New(config *Config, logger log.Logger) (store Store, err error) {
	switch config.name {
	case "remote":
		store = newRemoteStore(config.size, config.remoteConfig, logger)
	case "local":
		store = newVirtualStore(config.size)
	case "nop":
		store = newNopStore()
	default:
		err = errors.Errorf("unexpected store type %q", config.name)
	}
	return
}

// RequiresRemoteConfig states if the remote configuration setup is required.
func RequiresRemoteConfig(name string) bool {
	return name == "remote"
}
