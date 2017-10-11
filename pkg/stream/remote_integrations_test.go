// +build integration

package stream

import (
	"math/rand"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/uuid"
)

const (
	defaultAWSID     = ""
	defaultAWSSecret = ""
	defaultAWSToken  = ""
	defaultAWSRegion = "eu-west-1"
	defaultAWSStream = ""
)

func TestRemoteStream_Integration(t *testing.T) {
	// Don't run this in parallel

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	config, err := BuildConfig(
		WithRegion(GetEnv("AWS_FIREHOSE_REGION", defaultAWSRegion)),
		WithID(GetEnv("AWS_FIREHOSE_ID", defaultAWSID)),
		WithSecret(GetEnv("AWS_FIREHOSE_SECRET", defaultAWSSecret)),
		WithToken(GetEnv("AWS_FIREHOSE_TOKEN", defaultAWSToken)),
		WithStream(GetEnv("AWS_FIREHOSE_STREAM", defaultAWSStream)),
		WithMaxNumberOfMessages(10),
		WithVisibilityTimeout(time.Second*100),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("new", func(t *testing.T) {
		stream, err := NewRemoteStream(config, log.NewLogfmtLogger(os.Stdout))
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := false, stream == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("add a value then commit", func(t *testing.T) {
		stream, err := NewRemoteStream(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		var (
			sid = uuid.MustNew(rnd)
			rid = uuid.MustNew(rnd)
		)

		segment := &testSegment{
			id: sid,
			records: []queue.Record{
				queue.Record{
					ID:        rid,
					MessageID: uuid.MustNew(rnd).String(),
					Body:      []byte("hello, world!"),
				},
			},
		}

		err = stream.Append(segment)
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}

		transaction := NewTransaction()
		transaction.Set(sid, []uuid.UUID{
			rid,
		})

		err = stream.Commit(transaction)
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("add a value then commit with all", func(t *testing.T) {
		stream, err := NewRemoteStream(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		var (
			sid = uuid.MustNew(rnd)
			rid = uuid.MustNew(rnd)
		)

		segment := &testSegment{
			id: sid,
			records: []queue.Record{
				queue.Record{
					ID:        rid,
					MessageID: uuid.MustNew(rnd).String(),
					Body:      []byte("hello, world!"),
				},
			},
		}

		err = stream.Append(segment)
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}

		err = stream.Commit(All())
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("add a value then failed", func(t *testing.T) {
		stream, err := NewRemoteStream(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		var (
			sid = uuid.MustNew(rnd)
			rid = uuid.MustNew(rnd)
		)

		segment := &testSegment{
			id: sid,
			records: []queue.Record{
				queue.Record{
					ID:        rid,
					MessageID: uuid.MustNew(rnd).String(),
					Body:      []byte("hello, world!"),
				},
			},
		}

		err = stream.Append(segment)
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}

		transaction := NewTransaction()
		transaction.Set(sid, []uuid.UUID{
			rid,
		})

		err = stream.Failed(transaction)
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("add a value then failed with all", func(t *testing.T) {
		stream, err := NewRemoteStream(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		var (
			sid = uuid.MustNew(rnd)
			rid = uuid.MustNew(rnd)
		)

		segment := &testSegment{
			id: sid,
			records: []queue.Record{
				queue.Record{
					ID:        rid,
					MessageID: uuid.MustNew(rnd).String(),
					Body:      []byte("hello, world!"),
				},
			},
		}

		err = stream.Append(segment)
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}

		err = stream.Failed(All())
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
