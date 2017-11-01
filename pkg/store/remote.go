package store

import (
	"encoding/json"
	"math/rand"
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/trussle/courier/pkg/store/client"
	"github.com/trussle/courier/pkg/store/cluster"
	"github.com/trussle/courier/pkg/store/fifo"
)

type remoteStore struct {
	fifo              *fifo.FIFO
	client            *client.Client
	peer              cluster.Peer
	replicationFactor int
	logger            log.Logger
}

func newRemoteStore(size, replicationFactor int, peer cluster.Peer, logger log.Logger) Store {
	store := &remoteStore{
		client:            client.NewClient(http.DefaultClient),
		peer:              peer,
		replicationFactor: replicationFactor,
		logger:            logger,
	}
	store.fifo = fifo.NewFIFO(size, store.onElementEviction)
	return store
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

	var identifiers map[string]struct{}
	identifiers, err = v.gather(instances, idents)
	if err != nil {
		return
	}

	// Intersection
	for _, v := range idents {
		if _, ok := identifiers[v]; ok {
			union = append(union, v)
		} else {
			difference = append(difference, v)
		}
	}

	return
}

func (v *remoteStore) onElementEviction(reason fifo.EvictionReason, key string) {
	// do nothing
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

	for _, ident := range idents {
		if !v.fifo.Add(ident) {
			log.With(v.logger).Log("action", "add failure")
		}
	}

	return nil
}

func (v *remoteStore) gather(instances, idents []string) (map[string]struct{}, error) {
	body, err := json.Marshal(IngestInput{
		Identifiers: idents,
	})
	if err != nil {
		return nil, err
	}

	var (
		numInstances = len(instances)
		indices      = rand.Perm(numInstances)
		replicated   = 0
		identifiers  = make(map[string]struct{})
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

		var input IngestInput
		if err := json.Unmarshal(resp, &input); err != nil {
			continue
		}

		// Remove duplications
		for _, v := range input.Identifiers {
			identifiers[v] = struct{}{}
		}

		replicated++
	}

	if replicated < v.replicationFactor {
		return nil, errors.Errorf("failed to fully replicate")
	}

	// Sum internal fifo values
	if err = v.fifo.Walk(func(id string) error {
		identifiers[id] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}

	return identifiers, nil
}
