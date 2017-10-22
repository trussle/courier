package models_test

import (
	"testing"
	"testing/quick"

	"github.com/trussle/courier/pkg/models"
)

func TestReceipt(t *testing.T) {
	t.Parallel()

	t.Run("string", func(t *testing.T) {
		fn := func(s string) bool {
			return models.Receipt(s).String() == s
		}
		if err := quick.Check(fn, nil); err != nil {
			t.Error(err)
		}
	})
}
