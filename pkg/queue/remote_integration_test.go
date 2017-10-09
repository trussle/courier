// +build integration

package queue

import (
	"math/rand"
	"syscall"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
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

	config, err := BuildConfig(
		WithRegion(GetEnv("AWS_SQS_REGION", defaultAWSRegion)),
		WithID(GetEnv("AWS_SQS_ID", defaultAWSID)),
		WithSecret(GetEnv("AWS_SQS_SECRET", defaultAWSSecret)),
		WithToken(GetEnv("AWS_SQS_TOKEN", defaultAWSToken)),
		WithQueue(GetEnv("AWS_SQS_QUEUE", defaultAWSQueue)),
		WithMaxNumberOfMessages(10),
		WithVisibilityTimeout(time.Second*100),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("new", func(t *testing.T) {
		queue, err := NewRemoteQueue(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := false, queue == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("enqueue a value", func(t *testing.T) {
		queue, err := NewRemoteQueue(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		rec := Record{
			ID:   uuid.MustNew(rnd),
			Body: []byte("hello, world!"),
		}

		err = queue.Enqueue(rec)
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("dequeue a value", func(t *testing.T) {
		queue, err := NewRemoteQueue(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		enqueueSegment(t, rnd, queue, "hello, world!")

		segment, err := queue.Dequeue()
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := false, segment.ID().Zero(); expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}

		size := segment.Size()
		if expected, actual := 1, size; actual < expected {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		var ids []uuid.UUID
		segment.Walk(func(r Record) error {
			ids = append(ids, r.ID)
			return nil
		})

		if _, err = segment.Commit(ids); err != nil {
			t.Error(err)
		}
	})

	t.Run("dequeue size after walk should be grater than 1", func(t *testing.T) {
		queue, err := NewRemoteQueue(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		body := "hello, world!"
		enqueueSegment(t, rnd, queue, body)

		segment, err := queue.Dequeue()
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := false, segment.ID().Zero(); actual != expected {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}

		var ids []uuid.UUID
		err = segment.Walk(func(r Record) error {
			ids = append(ids, r.ID)

			if expected, actual := 0, len(r.Body); actual == expected {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			return nil
		})
		if err != nil {
			t.Error(err)
		}

		size := segment.Size()
		if expected, actual := 1, size; actual < expected {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		if _, err = segment.Commit(ids); err != nil {
			t.Error(err)
		}
	})

	t.Run("enqueue multiple then dequeue and walk after failure", func(t *testing.T) {
		queue, err := NewRemoteQueue(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		body := "hello, world!"
		for i := 0; i < 10; i++ {
			enqueueSegment(t, rnd, queue, body)
		}

		segment, err := queue.Dequeue()
		if err != nil {
			t.Fatal(err)
		}

		size0 := segment.Size()
		if size0 == 0 {
			t.Skip("Nothing to test in this zero scenario")
			return
		}

		err = segment.Walk(func(r Record) error {
			if expected, actual := 0, len(r.Body); actual == expected {
				t.Errorf("expected: %d, actual: %d", expected, actual)
			}
			return errors.New("bad")
		})
		if err == nil {
			t.Fatalf("expected error, received nil")
		}

		size1 := segment.Size()
		if expected, actual := size0, size1; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}

		var ids []uuid.UUID
		segment.Walk(func(r Record) error {
			ids = append(ids, r.ID)
			return nil
		})
		if _, err = segment.Commit(ids); err != nil {
			t.Error(err)
		}
	})

	t.Run("change visibility with no records", func(t *testing.T) {
		queue, err := newRemoteQueue(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		err = queue.changeMessageVisibility(make(Records, 0))
		if expected, actual := true, err == nil; actual != expected {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("change visibility with no timeout", func(t *testing.T) {
		config, err := BuildConfig(
			WithRegion(GetEnv("AWS_REGION", defaultAWSRegion)),
			WithID(GetEnv("AWS_ID", defaultAWSID)),
			WithSecret(GetEnv("AWS_SECRET", defaultAWSSecret)),
			WithToken(GetEnv("AWS_TOKEN", defaultAWSToken)),
			WithQueue(GetEnv("AWS_QUEUE", defaultAWSQueue)),
			WithMaxNumberOfMessages(10),
			WithVisibilityTimeout(0),
		)
		if err != nil {
			t.Fatal(err)
		}

		queue, err := newRemoteQueue(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		recs := make(Records, 1)
		recs.Append(Record{})

		err = queue.changeMessageVisibility(recs)
		if expected, actual := true, err == nil; actual != expected {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("commit with no records", func(t *testing.T) {
		queue, err := newRemoteQueue(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		segment := newRealSegment(uuid.MustNew(rnd), queue, make([]Record, 0), log.NewNopLogger())

		_, err = segment.Commit(make([]uuid.UUID, 0))
		if expected, actual := true, err == nil; actual != expected {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
		if expected, actual := 0, segment.Size(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("failed with no records", func(t *testing.T) {
		queue, err := newRemoteQueue(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		segment := newRealSegment(uuid.MustNew(rnd), queue, make([]Record, 0), log.NewNopLogger())

		_, err = segment.Failed(make([]uuid.UUID, 0))
		if expected, actual := true, err == nil; actual != expected {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
		if expected, actual := 0, segment.Size(); expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("failed with some records", func(t *testing.T) {
		queue, err := newRemoteQueue(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		segment := newRealSegment(uuid.MustNew(rnd), queue, make([]Record, 1), log.NewNopLogger())

		_, err = segment.Failed(make([]uuid.UUID, 0))
		if expected, actual := true, err == nil; actual != expected {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
		if expected, actual := 0, segment.Size(); expected != actual {
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
