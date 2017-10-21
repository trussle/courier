package generator

type Generator interface {
	Run()

	Stop()

	Dequeue() <-chan Record

	Commit(Transaction) (Result, error)

	Failed(Transaction) (Result, error)
}
