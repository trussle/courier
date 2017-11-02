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
		if !v.fifo.Contains(ident) {
			v.fifo.Add(ident)
		}
	}
	return nil
}

func (v *virtualStore) Intersection(idents []string) ([]string, []string, error) {
	var (
		union      = make([]string, 0)
		difference = make([]string, 0)
	)

	for _, ident := range unique(idents) {
		if v.fifo.Contains(ident) {
			union = append(union, ident)
		} else {
			difference = append(difference, ident)
		}
	}
	return union, difference, nil
}

func (v *virtualStore) onElementEviction(reason fifo.EvictionReason, key string) {
	// do nothing
}

func unique(a []string) []string {
	unique := make(map[string]struct{})
	for _, k := range a {
		unique[k] = struct{}{}
	}

	res := make([]string, 0)
	for k := range unique {
		res = append(res, k)
	}
	return res
}
