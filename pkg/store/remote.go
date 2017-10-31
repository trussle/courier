package store

import (
	"github.com/trussle/courier/pkg/store/fifo"
)

type remoteStore struct {
	fifo              *fifo.FIFO
	replicationFactor int
}

func newRemoteStore(size int) Store {
	store := &remoteStore{}
	store.fifo = fifo.NewFIFO(size, store.onElementEviction)
	return store
}

func (v *remoteStore) Add(idents []string) error {
	for _, ident := range idents {
		v.fifo.Add(ident)
	}

	return nil
}

func (v *remoteStore) Intersection(idents []string) (union, difference []string, err error) {
	for _, ident := range idents {
		if v.fifo.Contains(ident) {
			union = append(union, ident)
		} else {
			difference = append(difference, ident)
		}
	}
	return
}

func (v *remoteStore) onElementEviction(reason fifo.EvictionReason, key string) {
	// do nothing
}
