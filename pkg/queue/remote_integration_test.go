// +build integration

package queue_test

import (
	"math/rand"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/trussle/courier/pkg/models"
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/uuid"
)

const (
	defaultAWSID     = ""
	defaultAWSSecret = ""
	defaultAWSToken  = ""
	defaultAWSRegion = "eu-west-1"
	defaultAWSQueue  = ""
)

func TestRemoteQueue_Integration(t *testing.T) {
	// Don't run this in parallel

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	remoteConfig, err := queue.BuildConfig(
		queue.WithEC2Role(false),
		queue.WithRegion(GetEnv("AWS_REGION", defaultAWSRegion)),
		queue.WithID(GetEnv("AWS_ID", defaultAWSID)),
		queue.WithSecret(GetEnv("AWS_SECRET", defaultAWSSecret)),
		queue.WithToken(GetEnv("AWS_TOKEN", defaultAWSToken)),
		queue.WithQueue(GetEnv("AWS_SQS_QUEUE", defaultAWSQueue)),
		queue.WithMaxNumberOfMessages(10),
		queue.WithVisibilityTimeout(time.Second*100),
	)
	if err != nil {
		t.Fatal(err)
	}

	config, err := queue.Build(
		queue.With("remote"),
		queue.WithConfig(remoteConfig),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("new", func(t *testing.T) {
		queue, err := queue.New(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := false, queue == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("enqueue a value", func(t *testing.T) {
		queue, err := queue.New(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		go queue.Run()
		defer queue.Stop()

		rec := TestRecord{
			id:   uuid.MustNew(rnd),
			body: []byte("hello, world!"),
		}

		err = queue.Enqueue(rec)
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func GetEnv(key string, defaultValue string) (value string) {
	var ok bool
	if value, ok = syscall.Getenv(key); ok {
		return
	}
	return defaultValue
}

type TestRecord struct {
	id        uuid.UUID
	messageID string
	receipt   models.Receipt
	body      []byte
	timestamp time.Time
}

func (t TestRecord) ID() uuid.UUID {
	return t.id
}

func (t TestRecord) Body() []byte {
	return t.body
}

func (t TestRecord) Receipt() models.Receipt {
	return t.receipt
}

func (t TestRecord) Commit(txn models.Transaction) error {
	return txn.Push(t.id, t.receipt)
}

func (t TestRecord) Failed(txn models.Transaction) error {
	return txn.Push(t.id, t.receipt)
}

// Equal checks the equality of records against each other
func (t TestRecord) Equal(other queue.Record) bool {
	return t.ID().Equal(other.ID()) &&
		reflect.DeepEqual(t.Body(), other.Body())
}
