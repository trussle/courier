package generator

import (
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/trussle/courier/pkg/uuid"
)

// RemoteConfig creates a configuration to create a RemoteQueue.
type RemoteConfig struct {
	ID, Secret, Token   string
	Region, Queue       string
	MaxNumberOfMessages int64
	VisibilityTimeout   time.Duration
}

type remoteGenerator struct {
	client              *sqs.SQS
	queueURL            *string
	maxNumberOfMessages *int64
	waitTime            *int64
	visibilityTimeout   *int64
	stop                chan chan struct{}
	records             chan Record
	randSource          *rand.Rand
	logger              log.Logger
}

func newRemoteGenerator(config *RemoteConfig, logger log.Logger) Generator {
	return &remoteGenerator{
		stop:       make(chan chan struct{}),
		records:    make(chan Record),
		randSource: rand.New(rand.NewSource(time.Now().UnixNano())),
		logger:     logger,
	}
}

func (v *remoteGenerator) Watch() <-chan Record {
	return v.records
}

func (v *remoteGenerator) Run() {
	step := time.NewTicker(10 * time.Millisecond)
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

			unique := make(map[string]Record, len(resp.Messages))
			for _, msg := range resp.Messages {
				id, e := uuid.New(v.randSource)
				if e != nil {
					continue
				}

				unique[aws.StringValue(msg.MessageId)] = newRemoteRecord(
					id,
					aws.StringValue(msg.MessageId),
					Receipt(aws.StringValue(msg.ReceiptHandle)),
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

func (v *remoteGenerator) Stop() {
	q := make(chan struct{})
	v.stop <- q
	<-q
}

func (v *remoteGenerator) Commit(txn Transaction) (Result, error) {
	records := make(map[uuid.UUID]Receipt)
	if err := txn.Walk(func(id uuid.UUID, receipt Receipt) error {
		records[id] = receipt
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
	}, txn.Flush()
}

func (v *remoteGenerator) Failed(txn Transaction) (Result, error) {
	return Result{}, txn.Flush()
}

func (v *remoteGenerator) changeMessageVisibility(records map[string]Record) error {
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

type remoteRecord struct {
	id         uuid.UUID
	messageId  string
	receipt    Receipt
	body       []byte
	recievedAt time.Time
}

func newRemoteRecord(id uuid.UUID,
	messageId string,
	receipt Receipt,
	body []byte,
	recievedAt time.Time,
) Record {
	return &remoteRecord{
		id:         id,
		messageId:  messageId,
		receipt:    receipt,
		body:       body,
		recievedAt: recievedAt,
	}
}

func (r *remoteRecord) ID() uuid.UUID {
	return r.id
}

func (r *remoteRecord) Receipt() Receipt {
	return r.receipt
}

func (r *remoteRecord) Commit(txn Transaction) error {
	return txn.Push(r.id, r.receipt)
}

func (r *remoteRecord) Failed(txn Transaction) error {
	return txn.Push(r.id, r.receipt)
}
