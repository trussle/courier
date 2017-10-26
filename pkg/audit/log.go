package audit

import (
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/trussle/courier/pkg/models"
)

// Log represents an audit log of transactions that have occurred.
type Log interface {

	// Append a transaction to the log
	Append(models.Transaction) error
}

// Config encapsulates the requirements for generating a Stream
type Config struct {
	name         string
	remoteConfig *RemoteConfig
	localConfig  *LocalConfig
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

// WithRemoteConfig adds a remote log config to the configuration
func WithRemoteConfig(remoteConfig *RemoteConfig) Option {
	return func(config *Config) error {
		config.remoteConfig = remoteConfig
		return nil
	}
}

// WithLocalConfig adds a local log config to the configuration
func WithLocalConfig(localConfig *LocalConfig) Option {
	return func(config *Config) error {
		config.localConfig = localConfig
		return nil
	}
}

// New returns a new log
func New(config *Config, logger log.Logger) (log Log, err error) {
	switch config.name {
	case "remote":
		log, err = newRemoteLog(config.remoteConfig, logger)
	case "local":
		log, err = newLocalLog(config.localConfig, logger)
	case "nop":
		log = newNopLog()
	default:
		err = errors.Errorf("unexpected queue type %q", config.name)
	}
	return
}
