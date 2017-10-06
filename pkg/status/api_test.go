package status

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/kit/log"
)

func TestAPI(t *testing.T) {
	t.Parallel()

	t.Run("liveness", func(t *testing.T) {
		var (
			api    = NewAPI(log.NewNopLogger())
			server = httptest.NewServer(api)
		)
		defer server.Close()

		response, err := http.Get(fmt.Sprintf("%s/health", server.URL))
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := http.StatusOK, response.StatusCode; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})

	t.Run("readiness", func(t *testing.T) {
		var (
			api    = NewAPI(log.NewNopLogger())
			server = httptest.NewServer(api)
		)
		defer server.Close()

		response, err := http.Get(fmt.Sprintf("%s/ready", server.URL))
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := http.StatusOK, response.StatusCode; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})
}
