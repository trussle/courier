package audit

import (
	"testing"

	"github.com/trussle/courier/pkg/queue"
)

func TestNop(t *testing.T) {
	t.Parallel()

	t.Run("append", func(t *testing.T) {
		log := newNopLog()
		err := log.Append(queue.NewTransaction())

		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t, err: %v", expected, actual, err)
		}
	})
}
