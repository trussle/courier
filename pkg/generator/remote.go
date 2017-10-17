package generator

import (
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-kit/kit/log"
	"github.com/trussle/courier/pkg/records"
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
	records             chan records.Record
	randSource          *rand.Rand
}

func newRemoteGenerator(config *RemoteConfig, logger log.Logger) Generator {
	return &remoteGenerator{
		stop:       make(chan chan struct{}),
		records:    make(chan records.Record),
		randSource: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (v *remoteGenerator) Watch() <-chan records.Record {
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

			for _, msg := range resp.Messages {
				id, e := uuid.New(v.randSource)
				if e != nil {
					continue
				}

				v.records <- records.Record{
					ID:        id,
					MessageID: aws.StringValue(msg.MessageId),
					Receipt:   aws.StringValue(msg.ReceiptHandle),
					Body:      []byte(aws.StringValue(msg.Body)),
					Timestamp: time.Now(),
				}
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
