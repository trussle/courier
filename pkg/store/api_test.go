package store

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
	metricMocks "github.com/trussle/courier/pkg/metrics/mocks"
)

func TestAPI(t *testing.T) {
	t.Parallel()

	t.Run("not found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			clients  = metricMocks.NewMockGauge(ctrl)
			duration = metricMocks.NewMockHistogramVec(ctrl)
			observer = metricMocks.NewMockObserver(ctrl)
			api      = NewAPI(newNopStore(), log.NewNopLogger(), clients, duration)
			server   = httptest.NewServer(api)
		)
		defer server.Close()

		clients.EXPECT().Inc().Times(1)
		clients.EXPECT().Dec().Times(1)

		duration.EXPECT().WithLabelValues("GET", "/bad", "404").Return(observer).Times(1)
		observer.EXPECT().Observe(Float64()).Times(1)

		response, err := http.Get(fmt.Sprintf("%s/bad", server.URL))
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := http.StatusNotFound, response.StatusCode; expected != actual {
			t.Errorf("expected: %d, actual: %d", expected, actual)
		}
	})
}

type float64Matcher struct{}

func (float64Matcher) Matches(x interface{}) bool {
	_, ok := x.(float64)
	return ok
}

func (float64Matcher) String() string {
	return "is float64"
}

func Float64() gomock.Matcher { return float64Matcher{} }
