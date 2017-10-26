package queue

import (
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/uuid"
)

const (
	defaultRunFrequency = time.Millisecond * 1
)

// RemoteConfig creates a configuration to create a RemoteQueue.
type RemoteConfig struct {
	EC2Role             bool
	ID, Secret, Token   string
	Region, Queue       string
	MaxNumberOfMessages int64
	VisibilityTimeout   time.Duration
}

type remoteQueue struct {
	client              *sqs.SQS
	queueURL            *string
	maxNumberOfMessages *int64
	waitTime            *int64
	visibilityTimeout   *int64
	freq                time.Duration
	stop                chan chan struct{}
	records             chan models.Record
	randSource          *rand.Rand
	logger              log.Logger
}

func newRemoteQueue(config *RemoteConfig, logger log.Logger) (Queue, error) {
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
		client = sqs.New(session.New(cfg))
	)

	// Attempt to get the queueURL
	queueURL, err := client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(config.Queue),
	})
	if err != nil {
		return nil, err
	}

	return &remoteQueue{
		client:              client,
		queueURL:            queueURL.QueueUrl,
		maxNumberOfMessages: aws.Int64(config.MaxNumberOfMessages),
		visibilityTimeout:   aws.Int64(int64(config.VisibilityTimeout)),
		freq:                defaultRunFrequency,
		stop:                make(chan chan struct{}),
		records:             make(chan models.Record),
		randSource:          rand.New(rand.NewSource(time.Now().UnixNano())),
		logger:              logger,
	}, nil
}

func (v *remoteQueue) Enqueue(rec models.Record) error {
	input := &sqs.SendMessageInput{
		MessageBody: aws.String(string(rec.Body())),
		QueueUrl:    v.queueURL,
	}
	_, err := v.client.SendMessage(input)
	return err
}

func (v *remoteQueue) Dequeue() <-chan models.Record {
	return v.records
}

func (v *remoteQueue) Run() {
	step := time.NewTicker(v.freq)
	defer step.Stop()

	for {
		select {
		case <-step.C:
			input := &sqs.ReceiveMessageInput{
				QueueUrl:            v.queueURL,
				MaxNumberOfMessages: v.maxNumberOfMessages,
				MessageAttributeNames: []*string{
					aws.String("All"),
				},
				WaitTimeSeconds: v.waitTime,
			}

			resp, err := v.client.ReceiveMessage(input)
			if err != nil {
				continue
			}

			unique := make(map[string]models.Record, len(resp.Messages))
			for _, msg := range resp.Messages {
				id, e := uuid.New(v.randSource)
				if e != nil {
					continue
				}

				unique[aws.StringValue(msg.MessageId)] = NewRecord(
					id,
					aws.StringValue(msg.MessageId),
					models.Receipt(aws.StringValue(msg.ReceiptHandle)),
					[]byte(aws.StringValue(msg.Body)),
					time.Now(),
				)
			}

			for _, r := range unique {
				v.records <- r
			}

		case q := <-v.stop:
			close(q)
			return
		}
	}
}

func (v *remoteQueue) Stop() {
	q := make(chan struct{})
	v.stop <- q
	<-q
}

func (v *remoteQueue) Commit(txn models.Transaction) (Result, error) {
	records := make(map[uuid.UUID]models.Receipt)
	if err := txn.Walk(func(id uuid.UUID, record models.Record) error {
		records[id] = record.Receipt()
		return nil
	}); err != nil {
		return Result{}, err
	}

	var (
		index    int
		entities = make([]*sqs.DeleteMessageBatchRequestEntry, len(records))
	)
	for k, v := range records {
		entities[index] = &sqs.DeleteMessageBatchRequestEntry{
			Id:            aws.String(k.String()),
			ReceiptHandle: aws.String(v.String()),
		}
		index++
	}

	input := &sqs.DeleteMessageBatchInput{
		Entries:  entities,
		QueueUrl: v.queueURL,
	}
	output, err := v.client.DeleteMessageBatch(input)
	if err != nil {
		return Result{}, err
	}

	return Result{
		Success: len(output.Successful),
		Failure: len(output.Failed),
	}, nil
}

func (v *remoteQueue) Failed(txn models.Transaction) (Result, error) {
	return Result{}, nil
}

func (v *remoteQueue) changeMessageVisibility(records map[string]models.Record) error {
	// fast exit
	if len(records) == 0 {
		return nil
	}

	var (
		timeout = *v.visibilityTimeout
		seconds = time.Duration(timeout) / time.Second
	)
	if timeout == 0 || seconds <= 0 {
		return nil
	}

	var (
		index   int
		entries = make([]*sqs.ChangeMessageVisibilityBatchRequestEntry, len(records))
	)
	for _, v := range records {
		entries[index] = &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(v.ID().String()),
			ReceiptHandle:     aws.String(v.Receipt().String()),
			VisibilityTimeout: aws.Int64(int64(seconds)),
		}
		index++
	}

	input := &sqs.ChangeMessageVisibilityBatchInput{
		Entries:  entries,
		QueueUrl: v.queueURL,
	}
	output, err := v.client.ChangeMessageVisibilityBatch(input)
	if err != nil {
		level.Warn(v.logger).Log("state", "visibility change", "err", err)
		return err
	}
	if num := len(output.Failed); num > 0 {
		level.Warn(v.logger).Log("state", "visibility change", "failed", num)
	}
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

// WithQueue adds an Queue option to the configuration
func WithQueue(queue string) ConfigOption {
	return func(config *RemoteConfig) error {
		config.Queue = queue
		return nil
	}
}

// WithMaxNumberOfMessages adds an MaxNumberOfMessages option to the
// configuration
func WithMaxNumberOfMessages(numOfMessages int64) ConfigOption {
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
