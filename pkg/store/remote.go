package store

import (
	"encoding/json"
	"math/rand"
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/trussle/courier/pkg/store/client"
	"github.com/trussle/courier/pkg/store/cluster"
)

type remoteStore struct {
	client            *client.Client
	peer              cluster.Peer
	replicationFactor int
	logger            log.Logger
}

func newRemoteStore(size, replicationFactor int, peer cluster.Peer, logger log.Logger) Store {
	return &remoteStore{
		client:            client.NewClient(http.DefaultClient),
		peer:              peer,
		replicationFactor: replicationFactor,
		logger:            logger,
	}
}

func (v *remoteStore) Add(idents []string) error {
	instances, err := v.storeInstances()
	if err != nil {
		return err
	}

	return v.replicate(instances, idents)
}

func (v *remoteStore) Intersection(idents []string) (union, difference []string, err error) {
	var instances []string
	instances, err = v.storeInstances()
	if err != nil {
		return
	}

	var intersections []Intersections
	intersections, err = v.gather(instances, idents)
	if err != nil {
		return
	}

	// Sum intersections
	for _, v := range intersections {
		union = append(union, v.Union...)
		difference = append(difference, v.Difference...)
	}

	return
}

func (v *remoteStore) storeInstances() ([]string, error) {
	instances, err := v.peer.Current(cluster.PeerTypeStore)
	if err != nil {
		return nil, err
	}

	// Zero instances, store locally.
	numInstances := len(instances)
	if numInstances == 0 {
		return nil, errors.Errorf("no instances")
	}
	if want, got := v.replicationFactor, numInstances; got < want {
		return nil, errors.Errorf("consensus replication factor")
	}

	return instances, nil
}

func (v *remoteStore) replicate(instances, idents []string) error {
	body, err := json.Marshal(IngestInput{
		Identifiers: idents,
	})
	if err != nil {
		return err
	}

	var (
		numInstances = len(instances)
		indices      = rand.Perm(numInstances)
		replicated   = 0
	)
	for i := 0; i < numInstances; i++ {
		var (
			index    = indices[i]
			instance = instances[index]
		)
		_, err := v.client.Post(instance, body)
		if err != nil {
			continue
		}
		replicated++
	}

	if replicated < v.replicationFactor {
		return errors.Errorf("failed to fully replicate")
	}

	return nil
}

func (v *remoteStore) gather(instances, idents []string) ([]Intersections, error) {
	body, err := json.Marshal(IngestInput{
		Identifiers: idents,
	})
	if err != nil {
		return nil, err
	}

	var (
		numInstances  = len(instances)
		indices       = rand.Perm(numInstances)
		replicated    = 0
		intersections = make([]Intersections, numInstances)
	)
	for i := 0; i < numInstances; i++ {
		var (
			index    = indices[i]
			instance = instances[index]
		)
		resp, err := v.client.Post(instance, body)
		if err != nil {
			continue
		}

		var input Intersections
		if err := json.Unmarshal(resp, &input); err != nil {
			continue
		}

		intersections[i] = input

		replicated++
	}

	if replicated < v.replicationFactor {
		return nil, errors.Errorf("failed to fully replicate")
	}

	return intersections, nil
}

type Intersections struct {
	Union      []string `json:"union"`
	Difference []string `json:"difference"`
}
