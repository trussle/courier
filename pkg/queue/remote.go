package queue

import (
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/trussle/courier/pkg/uuid"
)

// RemoteConfig creates a configuration to create a RemoteQueue.
type RemoteConfig struct {
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
	randSource          *rand.Rand
	logger              log.Logger
}

// NewRemoteQueue creates a new remote peer that abstracts over a SQS queue.
func NewRemoteQueue(config *RemoteConfig, logger log.Logger) (Queue, error) {
	return newRemoteQueue(config, logger)
}

func newRemoteQueue(config *RemoteConfig, logger log.Logger) (*remoteQueue, error) {
	creds := credentials.NewChainCredentials(
		[]credentials.Provider{
			&credentials.EnvProvider{},
			&credentials.StaticProvider{
				Value: credentials.Value{
					AccessKeyID:     config.ID,
					SecretAccessKey: config.Secret,
					SessionToken:    config.Token,
				},
			},
		},
	)
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
		randSource:          rand.New(rand.NewSource(time.Now().UnixNano())),
		logger:              logger,
	}, nil
}

func (q *remoteQueue) Enqueue(r Record) error {
	input := &sqs.SendMessageInput{
		MessageBody: aws.String(string(r.Body)),
		QueueUrl:    q.queueURL,
	}
	_, err := q.client.SendMessage(input)
	return err
}

func (q *remoteQueue) Dequeue() (Segment, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:            q.queueURL,
		MaxNumberOfMessages: q.maxNumberOfMessages,
		MessageAttributeNames: []*string{
			aws.String("All"),
		},
		WaitTimeSeconds: q.waitTime,
	}

	resp, err := q.client.ReceiveMessage(input)
	if err != nil {
		return nil, err
	}

	records := make(Records, len(resp.Messages))
	for k, v := range resp.Messages {
		id, e := uuid.New(q.randSource)
		if e != nil {
			return nil, e
		}

		records[k] = Record{
			ID:        id,
			MessageID: aws.StringValue(v.MessageId),
			Receipt:   aws.StringValue(v.ReceiptHandle),
			Body:      []byte(aws.StringValue(v.Body)),
			Timestamp: time.Now(),
		}
	}

	if err = q.changeMessageVisibility(records); err != nil {
		return nil, err
	}

	id, err := uuid.New(q.randSource)
	if err != nil {
		return nil, err
	}

	return newRealSegment(
		id,
		q,
		records,
		log.With(q.logger, "component", "read_segment"),
	), nil
}

func (q *remoteQueue) Reset() error {
	input := &sqs.PurgeQueueInput{
		QueueUrl: q.queueURL,
	}

	_, err := q.client.PurgeQueue(input)
	return err
}

func (q *remoteQueue) changeMessageVisibility(records Records) error {
	// fast exit
	if records.Len() == 0 {
		return nil
	}

	var (
		timeout = *q.visibilityTimeout
		seconds = time.Duration(timeout) / time.Second
	)
	if timeout == 0 || seconds <= 0 {
		return nil
	}

	entries := make([]*sqs.ChangeMessageVisibilityBatchRequestEntry, records.Len())
	for k, v := range records {
		entries[k] = &sqs.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(v.ID.String()),
			ReceiptHandle:     aws.String(v.Receipt),
			VisibilityTimeout: aws.Int64(int64(seconds)),
		}
	}

	input := &sqs.ChangeMessageVisibilityBatchInput{
		Entries:  entries,
		QueueUrl: q.queueURL,
	}
	output, err := q.client.ChangeMessageVisibilityBatch(input)
	if err != nil {
		level.Warn(q.logger).Log("state", "visibility change", "err", err)
		return err
	}
	if num := len(output.Failed); num > 0 {
		level.Warn(q.logger).Log("state", "visibility change", "failed", num)
	}
	return nil
}

type realSegment struct {
	id      uuid.UUID
	queue   *remoteQueue
	records []Record
	read    []Record
	logger  log.Logger
}

func newRealSegment(
	id uuid.UUID,
	queue *remoteQueue,
	records []Record,
	logger log.Logger,
) Segment {
	return &realSegment{
		id:      id,
		queue:   queue,
		records: records,
		logger:  logger,
	}
}

func (r *realSegment) ID() uuid.UUID {
	return r.id
}

func (r *realSegment) Walk(fn func(r Record) error) (err error) {
	for _, rec := range r.records {
		if err = fn(rec); err != nil {
			break
		}
	}
	return
}

func (r *realSegment) Commit(ids []uuid.UUID) (Result, error) {
	if len(r.records) == 0 {
		return Result{0, 0}, nil
	}

	res, records := transactIDs(r.records, ids)

	entities := make([]*sqs.DeleteMessageBatchRequestEntry, len(records))
	for k, v := range records {
		entities[k] = &sqs.DeleteMessageBatchRequestEntry{
			Id:            aws.String(v.ID.String()),
			ReceiptHandle: aws.String(v.Receipt),
		}
	}

	input := &sqs.DeleteMessageBatchInput{
		Entries:  entities,
		QueueUrl: r.queue.queueURL,
	}
	output, err := r.queue.client.DeleteMessageBatch(input)
	if err != nil {
		return Result{0, 0}, err
	}

	if size := len(output.Failed); size > 0 {
		level.Warn(r.logger).Log("failed_size", size)
		// There is nothing we can do here, other than allow the queue to resend
		// them at a further time.
	}

	// Reset the records when done
	r.records = r.records[:0]

	return res, nil
}

func (r *realSegment) Failed(ids []uuid.UUID) (Result, error) {
	if len(r.records) == 0 {
		return Result{0, 0}, nil
	}

	// Nothing to do, but to reset everything.
	res, _ := transactIDs(r.records, ids)
	r.records = r.records[:0]
	return res, nil
}

func (r *realSegment) Size() int {
	return len(r.records)
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
