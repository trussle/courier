package audit

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
)

// RemoteConfig creates a configuration to create a RemoteLog.
type RemoteConfig struct {
	EC2Role           bool
	ID, Secret, Token string
	Region, Stream    string
}

// Log represents a series of active records
type remoteLog struct {
	client    *firehose.Firehose
	streamURL *string
	logger    log.Logger
}

// NewRemoteLog creates a new Log with a size and age to know when a
// Log is at a certain capacity
func newRemoteLog(config *RemoteConfig, logger log.Logger) (Log, error) {
	// If in EC2Role, attempt to get things from env or ec2role, else just use
	// static credentials...
	var creds *credentials.Credentials
	if config.EC2Role {
		creds = credentials.NewChainCredentials([]credentials.Provider{
			&credentials.EnvProvider{},
			&ec2rolecreds.EC2RoleProvider{
				Client: ec2metadata.New(session.New()),
			},
		})
	} else {
		creds = credentials.NewStaticCredentials(
			config.ID,
			config.Secret,
			config.Token,
		)
	}
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

	return &remoteLog{
		client:    client,
		streamURL: aws.String(config.Stream),
		logger:    logger,
	}, nil
}

func (r *remoteLog) Append(txn models.Transaction) error {
	// Serialize all the record data
	var data [][]byte
	if err := txn.Walk(func(id uuid.UUID, record models.Record) error {
		message := fmt.Sprintf("%s %s\n", record.RecordID(), string(record.Body()))
		data = append(data, []byte(message))
		return nil
	}); err != nil {
		return err
	}

	records := make([]*firehose.Record, len(data))
	for k, v := range data {
		records[k] = &firehose.Record{
			Data: v,
		}
	}

	input := &firehose.PutRecordBatchInput{
		DeliveryStreamName: r.streamURL,
		Records:            records,
	}

	_, err := r.client.PutRecordBatch(input)
	return err
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

// WithEC2Role adds an EC2Role option to the configuration
func WithEC2Role(ec2Role bool) ConfigOption {
	return func(config *RemoteConfig) error {
		config.EC2Role = ec2Role
		return nil
	}
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
