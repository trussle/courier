package store

import (
	"github.com/trussle/courier/pkg/store/fifo"
)

type virtualStore struct {
	fifo *fifo.FIFO
}

func newVirtualStore(size int) Store {
	store := &virtualStore{}
	store.fifo = fifo.NewFIFO(size, store.onElementEviction)
	return store
}

func (v *virtualStore) Add(idents []string) error {
	for _, ident := range idents {
		v.fifo.Add(ident)
	}
	return nil
}

func (v *virtualStore) Intersection(idents []string) (union, difference []string, err error) {
	for _, ident := range idents {
		if v.fifo.Contains(ident) {
			union = append(union, ident)
		} else {
			difference = append(difference, ident)
		}
	}
	return
}

func (v *virtualStore) onElementEviction(reason fifo.EvictionReason, key string) {
	// do nothing
}