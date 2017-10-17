package generator

import "github.com/trussle/courier/pkg/records"

type Generator interface {
	Run()

	Stop()

	Watch() <-chan records.Record
}
