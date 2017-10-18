package generator

type Generator interface {
	Run()

	Stop()

	Watch() <-chan Record

	Commit(Transaction) (Result, error)

	Failed(Transaction) (Result, error)
}
