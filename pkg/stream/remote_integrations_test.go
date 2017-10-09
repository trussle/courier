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

type testSegment struct {
	id      uuid.UUID
	records []queue.Record
}

func newVirtualSegment(id uuid.UUID, records []queue.Record) *testSegment {
	return &testSegment{
		id:      id,
		records: records,
	}
}

func (v *testSegment) ID() uuid.UUID {
	return v.id
}

func (v *testSegment) Walk(fn func(queue.Record) error) (err error) {
	for _, rec := range v.records {
		if err = fn(rec); err != nil {
			break
		}
	}
	return
}

func (v *testSegment) Commit(ids []uuid.UUID) (queue.Result, error) {
	if len(v.records) == 0 {
		return queue.Result{0, 0}, nil
	}

	result, _ := transactIDs(v.records, ids)
	v.records = v.records[:0]
	return result, nil
}

func (v *testSegment) Failed(ids []uuid.UUID) (queue.Result, error) {
	if len(v.records) == 0 {
		return queue.Result{0, 0}, nil
	}

	result, _ := transactIDs(v.records, ids)
	v.records = v.records[:0]
	return result, nil
}

func (v *testSegment) Size() int {
	return len(v.records)
}

func transactIDs(records []queue.Record, ids []uuid.UUID) (queue.Result, []queue.Record) {
	// Fast exit
	if len(ids) == 0 {
		return queue.Result{0, len(records)}, make([]queue.Record, 0)
	}

	// Match items that are not in the ids
	var replicate []queue.Record
	for _, v := range records {
		if contains(ids, v.ID) {
			replicate = append(replicate, v)
		}
	}

	replicated := len(replicate)
	return queue.Result{replicated, len(records) - replicated}, replicate
}
