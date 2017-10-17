package generator

import "github.com/trussle/courier/pkg/records"
import "time"

type virtualGenerator struct {
	freq    time.Duration
	stop    chan chan struct{}
	records chan records.Record
}

func newVirtualGenerator(freq time.Duration) Generator {
	return &virtualGenerator{
		freq:    freq,
		stop:    make(chan chan struct{}),
		records: make(chan records.Record),
	}
}

func (v *virtualGenerator) Watch() <-chan records.Record {
	return v.records
}

func (v *virtualGenerator) Run() {
	step := time.NewTicker(v.freq)
	defer step.Stop()

	for {
		select {
		case <-step.C:
			rec := records.Record{}
			v.records <- rec

		case q := <-v.stop:
			close(q)
			return
		}
	}
}

func (v *virtualGenerator) Stop() {
	q := make(chan struct{})
	v.stop <- q
	<-q
}
