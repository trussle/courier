package stream

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/uuid"
)

// RemoteConfig creates a configuration to create a RemoteStream.
type RemoteConfig struct {
	ID, Secret, Token   string
	Region, Stream      string
	MaxNumberOfMessages int
	VisibilityTimeout   time.Duration
}

// Stream represents a series of active records
type remoteStream struct {
	client      *firehose.Firehose
	streamURL   *string
	active      []queue.Segment
	activeSince time.Time
	targetSize  int
	targetAge   time.Duration
	logger      log.Logger
}

// NewRemoteStream creates a new Stream with a size and age to know when a
// Stream is at a certain capacity
func newRemoteStream(config *RemoteConfig, logger log.Logger) (*remoteStream, error) {
	creds := credentials.NewStaticCredentials(
		config.ID,
		config.Secret,
		config.Token,
	)
	if _, err := creds.Get(); err != nil {
		return nil, errors.Wrap(err, "invalid credentials")
	}

	var (
		cfg = aws.NewConfig().
			WithRegion(config.Region).
			WithCredentials(creds).
			WithCredentialsChainVerboseErrors(true)
		client = firehose.New(session.New(cfg))
	)

	return &remoteStream{
		client:      client,
		streamURL:   aws.String(config.Stream),
		active:      make([]queue.Segment, 0),
		activeSince: time.Time{},
		targetSize:  config.MaxNumberOfMessages,
		targetAge:   config.VisibilityTimeout,
		logger:      logger,
	}, nil
}

// Len returns the number of available active records with in the Stream
func (l *remoteStream) Len() int {
	return len(l.active)
}

// Reset empties the remoteStream and puts it to a valid known state
func (l *remoteStream) Reset() error {
	l.active = l.active[:0]
	l.activeSince = time.Time{}

	return nil
}

// Capacity defines if the remoteStream is at a capacity. This is defined as if the
// remoteStream is over the target or age.
func (l *remoteStream) Capacity() bool {
	return l.Len() >= l.targetSize ||
		!l.activeSince.IsZero() && time.Since(l.activeSince) >= l.targetAge
}

// Append adds a segment with records to the remoteStream
func (l *remoteStream) Append(segment queue.Segment) error {
	l.active = append(l.active, segment)
	if l.activeSince.IsZero() {
		l.activeSince = time.Now()
	}
	return nil
}

// Walk allows the walking over each record sequentially
func (l *remoteStream) Walk(fn func(queue.Segment) error) error {
	for _, segment := range l.active {
		if err := fn(segment); err != nil {
			return err
		}
	}
	return nil
}

// Commit commits all the segments so that we can delete messages from the queue
func (l *remoteStream) Commit(input *Transaction) error {
	return l.resetVia(input, Flushed)
}

// Failed fails all the segments to make sure that we no longer work on those
// messages
func (l *remoteStream) Failed(input *Transaction) error {
	return l.resetVia(input, Failed)
}

func (l *remoteStream) resetVia(input *Transaction, reason Extension) error {
	var segments []queue.Segment
	for _, segment := range l.active {
		var ids []uuid.UUID
		if input.All() {
			if err := segment.Walk(func(record queue.Record) error {
				ids = append(ids, record.ID)
				return nil
			}); err != nil {
				continue
			}
		} else {
			var ok bool
			if ids, ok = input.Get(segment.ID()); !ok {
				segments = append(segments, segment)
				continue
			}
		}

		switch reason {
		case Failed:
			if _, err := segment.Failed(ids); err != nil {
				return err
			}

		case Flushed:
			// Serialize all the record data
			var data [][]byte
			if err := segment.Walk(func(record queue.Record) error {
				message := fmt.Sprintf("%s %s\n", record.MessageID, string(record.Body))
				data = append(data, []byte(message))
				return nil
			}); err != nil {
				// Nothing to do here, but continue
				level.Warn(l.logger).Log("state", "flushing", "err", err.Error())
			}

			if _, err := segment.Commit(ids); err != nil {
				return err
			}

			records := make([]*firehose.Record, len(data))
			for k, v := range data {
				records[k] = &firehose.Record{
					Data: v,
				}
			}

			input := &firehose.PutRecordBatchInput{
				DeliveryStreamName: l.streamURL,
				Records:            records,
			}

			if _, err := l.client.PutRecordBatch(input); err != nil {
				// Nothing to do but continue
				level.Warn(l.logger).Log("state", "flushing", "err", err.Error())
			}
		}
	}

	l.active = segments
	l.activeSince = time.Time{}

	return nil
}

// ConfigOption defines a option for generating a RemoteConfig
type ConfigOption func(*RemoteConfig) error

// BuildConfig ingests configuration options to then yield a
// RemoteConfig, and return an error if it fails during configuring.
func BuildConfig(opts ...ConfigOption) (*RemoteConfig, error) {
	var config RemoteConfig
	for _, opt := range opts {
		err := opt(&config)
		if err != nil {
			return nil, err
		}
	}
	return &config, nil
}

// WithID adds an ID option to the configuration
func WithID(id string) ConfigOption {
	return func(config *RemoteConfig) error {
		config.ID = id
		return nil
	}
}

// WithSecret adds an Secret option to the configuration
func WithSecret(secret string) ConfigOption {
	return func(config *RemoteConfig) error {
		config.Secret = secret
		return nil
	}
}

// WithToken adds an Token option to the configuration
func WithToken(token string) ConfigOption {
	return func(config *RemoteConfig) error {
		config.Token = token
		return nil
	}
}

// WithRegion adds an Region option to the configuration
func WithRegion(region string) ConfigOption {
	return func(config *RemoteConfig) error {
		config.Region = region
		return nil
	}
}

// WithStream adds an Stream option to the configuration
func WithStream(stream string) ConfigOption {
	return func(config *RemoteConfig) error {
		config.Stream = stream
		return nil
	}
}

// WithMaxNumberOfMessages adds an MaxNumberOfMessages option to the
// configuration
func WithMaxNumberOfMessages(numOfMessages int) ConfigOption {
	return func(config *RemoteConfig) error {
		config.MaxNumberOfMessages = numOfMessages
		return nil
	}
}

// WithVisibilityTimeout adds an VisibilityTimeout option to the
// configuration
func WithVisibilityTimeout(visibilityTimeout time.Duration) ConfigOption {
	return func(config *RemoteConfig) error {
		config.VisibilityTimeout = visibilityTimeout
		return nil
	}
}
