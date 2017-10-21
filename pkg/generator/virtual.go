package generator

import "time"

type virtualGenerator struct {
	freq    time.Duration
	stop    chan chan struct{}
	records chan Record
	fn      func() Record
}

func newVirtualGenerator(freq time.Duration, fn func() Record) Generator {
	return &virtualGenerator{
		freq:    freq,
		stop:    make(chan chan struct{}),
		records: make(chan Record),
		fn:      fn,
	}
}

func (v *virtualGenerator) Dequeue() <-chan Record {
	return v.records
}

func (v *virtualGenerator) Run() {
	step := time.NewTicker(v.freq)
	defer step.Stop()

	for {
		select {
		case <-step.C:
			v.records <- v.fn()

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

func (v *virtualGenerator) Commit(txn Transaction) (Result, error) {
	return Result{txn.Len(), 0}, txn.Flush()
}

func (v *virtualGenerator) Failed(txn Transaction) (Result, error) {
	return Result{txn.Len(), 0}, txn.Flush()
}
