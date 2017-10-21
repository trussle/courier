package generator

type nopGenerator struct{}

func newNopGenerator() Generator {
	return &nopGenerator{}
}

func (nopGenerator) Dequeue() <-chan Record {
	return make(chan Record)
}

func (nopGenerator) Run()  {}
func (nopGenerator) Stop() {}

func (nopGenerator) Commit(txn Transaction) (Result, error) {
	return Result{txn.Len(), 0}, txn.Flush()
}
func (nopGenerator) Failed(txn Transaction) (Result, error) {
	return Result{txn.Len(), 0}, txn.Flush()
}
