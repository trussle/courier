package store

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
	"github.com/trussle/courier/pkg/store/cluster"
	"github.com/trussle/courier/pkg/store/cluster/mocks"
)

func TestRemoteAdd(t *testing.T) {
	t.Parallel()

	t.Run("add without zero instances", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			mock   = mocks.NewMockPeer(ctrl)
			config = &RemoteConfig{
				ReplicationFactor: 10,
				Peer:              mock,
			}
			instances = []string{}
			store     = newRemoteStore(100, config, log.NewNopLogger())
		)

		mock.EXPECT().Current(cluster.PeerTypeStore).Return(instances, nil)

		err := store.Add([]string{"a", "b"})
		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("add without meeting replication factor", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			mock   = mocks.NewMockPeer(ctrl)
			config = &RemoteConfig{
				ReplicationFactor: 10,
				Peer:              mock,
			}
			instances = []string{
				"http://a.com",
				"http://b.com",
			}
			store = newRemoteStore(100, config, log.NewNopLogger())
		)

		mock.EXPECT().Current(cluster.PeerTypeStore).Return(instances, nil)

		err := store.Add([]string{"a", "b"})
		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("add with post failure", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			mock      = mocks.NewMockPeer(ctrl)
			instances = make([]string, 3)
			config    = &RemoteConfig{
				ReplicationFactor: len(instances),
				Peer:              mock,
			}
			store = newRemoteStore(2, config, log.NewNopLogger())
		)

		handle := func(k int) func(http.ResponseWriter, *http.Request) {
			return func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				if k == 0 {
					w.WriteHeader(http.StatusOK)
				} else {
					w.WriteHeader(http.StatusInternalServerError)
				}
			}
		}

		for k := range instances {
			mux := http.NewServeMux()
			mux.HandleFunc("/", handle(k))

			server := httptest.NewServer(mux)
			instances[k] = server.URL
		}

		mock.EXPECT().Current(cluster.PeerTypeStore).Return(instances, nil)

		err := store.Add([]string{"a", "b"})
		if expected, actual := false, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("add", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		var (
			mock      = mocks.NewMockPeer(ctrl)
			instances = make([]string, 3)
			config    = &RemoteConfig{
				ReplicationFactor: len(instances),
				Peer:              mock,
			}
			store = newRemoteStore(2, config, log.NewNopLogger())
		)

		for k := range instances {
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusOK)
			})

			server := httptest.NewServer(mux)
			instances[k] = server.URL
		}

		mock.EXPECT().Current(cluster.PeerTypeStore).Return(instances, nil)

		err := store.Add([]string{"a", "b"})
		if expected, actual := true, err == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})
}
