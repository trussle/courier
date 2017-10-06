package status

import (
	"encoding/json"
	"net/http"

	errs "github.com/trussle/courier/pkg/http"
	"github.com/go-kit/kit/log"
)

// These are the status API URL paths.
const (
	APIPathLivenessQuery  = "/health"
	APIPathReadinessQuery = "/ready"
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
	case method == "GET" && path == APIPathLivenessQuery:
		a.handleLiveness(w, r)
	case method == "GET" && path == APIPathReadinessQuery:
		a.handleReadiness(w, r)
	default:
		// Nothing found
		a.errors.NotFound(w, r)
	}
}

func (a *API) handleLiveness(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(struct{}{}); err != nil {
		a.errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (a *API) handleReadiness(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(struct{}{}); err != nil {
		a.errors.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type interceptingWriter struct {
	code int
	http.ResponseWriter
}

func (iw *interceptingWriter) WriteHeader(code int) {
	iw.code = code
	iw.ResponseWriter.WriteHeader(code)
}
