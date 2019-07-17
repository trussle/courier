// +build integration

package queue_test

import (
	"math/rand"
	"syscall"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/trussle/courier/pkg/queue"
)

const (
	defaultAWSID                  = ""
	defaultAWSSecret              = ""
	defaultAWSToken               = ""
	defaultAWSRegion              = "eu-west-1"
	defaultAWSQueue               = ""
	defaultAWSQueueOwnerAccountID = ""
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
		queue.WithQueueOwnerAWSAccountID(GetEnv("AWS_SQS_QUEUE_OWNER_ACCOUNT_ID", defaultAWSQueueOwnerAccountID)),
		queue.WithMaxNumberOfMessages(1),
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
		remote, err := queue.New(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		rec, err := queue.GenerateQueueRecord(rnd)
		if err != nil {
			t.Fatal(err)
		}

		err = remote.Enqueue(rec)
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("dequeue a value", func(t *testing.T) {
		remote, err := queue.New(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		rec, err := queue.GenerateQueueRecord(rnd)
		if err != nil {
			t.Fatal(err)
		}

		if err := remote.Enqueue(rec); err != nil {
			t.Fatal(err)
		}

		records, err := remote.Dequeue()
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := 1, len(records); expected > actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}

		for _, res := range records {
			if expected, actual := false, res.ID().Zero(); expected != actual {
				t.Errorf("expected: %t, actual: %t", expected, actual)
			}
		}
	})

	t.Run("dequeue a value and commit", func(t *testing.T) {
		remote, err := queue.New(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		rec, err := queue.GenerateQueueRecord(rnd)
		if err != nil {
			t.Fatal(err)
		}

		if err := remote.Enqueue(rec); err != nil {
			t.Fatal(err)
		}

		records, err := remote.Dequeue()
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := 1, len(records); expected > actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}

		txn := queue.NewTransaction()
		for _, res := range records {
			if err := txn.Push(res.ID(), res); err != nil {
				t.Fatal(err)
			}
		}

		result, err := remote.Commit(txn)
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := txn.Len(), result.Success; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := 0, result.Failure; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("dequeue a value and failed", func(t *testing.T) {
		remote, err := queue.New(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		rec, err := queue.GenerateQueueRecord(rnd)
		if err != nil {
			t.Fatal(err)
		}

		if err := remote.Enqueue(rec); err != nil {
			t.Fatal(err)
		}

		records, err := remote.Dequeue()
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := 1, len(records); expected > actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}

		txn := queue.NewTransaction()
		for _, res := range records {
			if err := txn.Push(res.ID(), res); err != nil {
				t.Fatal(err)
			}
		}

		result, err := remote.Failed(txn)
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := len(records), result.Success; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
		if expected, actual := 0, result.Failure; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
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
