package harness

import (
	"io/ioutil"
	"net/http"

	errs "github.com/trussle/courier/pkg/http"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

// These are the status API URL paths.
const (
	APIPathQuery = "/"
)

// API serves the status API
type API struct {
	logger log.Logger
	errors errs.Error
}

// NewAPI creates a API with the correct dependencies.
func NewAPI(logger log.Logger) *API {
	return &API{
		logger: logger,
		errors: errs.NewError(logger),
	}
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	iw := &interceptingWriter{http.StatusOK, w}
	w = iw

	// Routing table
	method, path := r.Method, r.URL.Path
	switch {
	case method == "POST" && path == APIPathQuery:
		a.handleQuery(w, r)
	default:
		// Nothing found
		a.errors.NotFound(w, r)
	}
}

func (a *API) handleQuery(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)

	level.Info(a.logger).Log("body", string(b))
}

type interceptingWriter struct {
	code int
	http.ResponseWriter
}

func (iw *interceptingWriter) WriteHeader(code int) {
	iw.code = code
	iw.ResponseWriter.WriteHeader(code)
}
