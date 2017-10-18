package queue

import (
	"math/rand"
	"testing"
	"testing/quick"
	"time"

	"github.com/pkg/errors"
	"github.com/trussle/courier/pkg/uuid"
)

func TestConfigBuild(t *testing.T) {
	t.Parallel()

	t.Run("build", func(t *testing.T) {
		fn := func(id, secret, token, region, queue string, numOfMessages, timeout int64) bool {
			config, err := BuildConfig(
				WithEC2Role(false),
				WithID(id),
				WithSecret(secret),
				WithToken(token),
				WithRegion(region),
				WithQueue(queue),
				WithMaxNumberOfMessages(numOfMessages),
				WithVisibilityTimeout(time.Duration(timeout)),
			)
			if err != nil {
				t.Fatal(err)
			}
			return config.ID == id &&
				config.Secret == secret &&
				config.Token == token &&
				config.Region == region &&
				config.Queue == queue &&
				config.MaxNumberOfMessages == numOfMessages &&
				config.VisibilityTimeout == time.Duration(timeout)
		}

		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})

	t.Run("invalid build", func(t *testing.T) {
		_, err := BuildConfig(
			func(config *RemoteConfig) error {
				return errors.Errorf("bad")
			},
		)

		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}

func enqueueSegment(t *testing.T, rnd *rand.Rand, queue Queue, body string) uuid.UUID {
	id := uuid.MustNew(rnd)

	rec := Record{
		ID:   id,
		Body: []byte(body),
	}

	if err := queue.Enqueue(rec); err != nil {
		t.Fatal(err)
	}

	return id
}
