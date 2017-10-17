package generator

import "github.com/trussle/courier/pkg/records"

type nopGenerator struct{}

func newNopGenerator() Generator {
	return &nopGenerator{}
}

func (nopGenerator) Watch() <-chan records.Record {
	return make(chan records.Record)
}

func (nopGenerator) Run()  {}
func (nopGenerator) Stop() {}
