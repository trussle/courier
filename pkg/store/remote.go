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

// RemoteConfig creates a configuration to create a RemoteLog.
type RemoteConfig struct {
	ReplicationFactor int
	Peer              cluster.Peer
}

type remoteStore struct {
	local             Store
	client            *client.Client
	peer              cluster.Peer
	replicationFactor int
	logger            log.Logger
}

func newRemoteStore(size int, config *RemoteConfig, logger log.Logger) Store {
	return &remoteStore{
		local:             newVirtualStore(size),
		client:            client.NewClient(http.DefaultClient),
		peer:              config.Peer,
		replicationFactor: config.ReplicationFactor,
		logger:            logger,
	}
}

func (v *remoteStore) Add(idents []string) error {
	instances, err := v.storeInstances()
	if err != nil {
		return err
	}

	if err := v.replicate(instances, idents); err != nil {
		return err
	}

	return v.local.Add(idents)
}

// union = matched
// difference = not matched
func (v *remoteStore) Intersection(idents []string) (union, difference []string, err error) {
	// Check typical exit clause.
	var localUnion, localDifference []string
	localUnion, localDifference, err = v.local.Intersection(idents)
	if len(filter(idents, localUnion)) == len(idents) {
		return
	}

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

	// Include local
	intersections = append(intersections, Intersections{
		Union:      localUnion,
		Difference: localDifference,
	})

	// Sum intersections
	var (
		uni  = map[string]struct{}{}
		diff = map[string]struct{}{}
	)
	for _, v := range intersections {
		for _, s := range filter(idents, v.Union) {
			uni[s] = struct{}{}
		}
		for _, s := range filter(idents, v.Difference) {
			diff[s] = struct{}{}
		}
	}

	for k := range uni {
		union = append(union, k)
	}
	for k := range diff {
		if _, ok := uni[k]; !ok {
			difference = append(difference, k)
		}
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

func filter(h []string, v []string) (res []string) {
	for _, a := range v {
		for _, b := range h {
			if a == b {
				res = append(res, b)
			}
		}
	}
	return
}

// ConfigOption defines a option for generating a RemoteConfig
type ConfigOption func(*RemoteConfig) error

// BuildConfig ingests configuration options to then yield a
// RemoteConfig, and return an error if it fails during configuring.
func BuildConfig(opts ...ConfigOption) (*RemoteConfig, error) {
	var config RemoteConfig
	for _, opt := range opts {
		err := opt(&config)
		if err != nil {
			return nil, err
		}
	}
	return &config, nil
}

// WithPeer adds an Peer option to the configuration
func WithPeer(peer cluster.Peer) ConfigOption {
	return func(config *RemoteConfig) error {
		config.Peer = peer
		return nil
	}
}

// WithReplicationFactor adds an ReplicationFactor option to the configuration
func WithReplicationFactor(factor int) ConfigOption {
	return func(config *RemoteConfig) error {
		config.ReplicationFactor = factor
		return nil
	}
}
