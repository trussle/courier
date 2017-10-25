PATH_COURIER = github.com/trussle/courier

UNAME_S := $(shell uname -s)
SED ?= sed -i
ifeq ($(UNAME_S),Darwin)
	SED += '' --
endif

.PHONY: all
all: install
	$(MAKE) clean build

.PHONY: install
install:
	go get github.com/Masterminds/glide
	go get github.com/mattn/goveralls
	go get github.com/golang/mock/mockgen
	go get github.com/prometheus/client_golang/prometheus
	glide install

.PHONY: build
build: dist/courier

dist/courier:
	go build -o dist/courier ${PATH_COURIER}/cmd/courier

pkg/audit/mocks/log.go:
	mockgen -package=mocks -destination=pkg/audit/mocks/log.go ${PATH_COURIER}/pkg/audit Log
	$(SED) 's/github.com\/trussle\/courier\/vendor\///g' ./pkg/audit/mocks/log.go

pkg/metrics/mocks/metrics.go:
	mockgen -package=mocks -destination=pkg/metrics/mocks/metrics.go ${PATH_COURIER}/pkg/metrics Gauge,HistogramVec,Counter
	$(SED) 's/github.com\/trussle\/courier\/vendor\///g' ./pkg/metrics/mocks/metrics.go

pkg/metrics/mocks/observer.go:
	mockgen -package=mocks -destination=pkg/metrics/mocks/observer.go github.com/prometheus/client_golang/prometheus Observer
	$(SED) 's/github.com\/trussle\/courier\/vendor\///g' ./pkg/metrics/mocks/observer.go

pkg/models/mocks/transaction.go:
	mockgen -package=mocks -destination=pkg/models/mocks/transaction.go ${PATH_COURIER}/pkg/models Transaction
	$(SED) 's/github.com\/trussle\/courier\/vendor\///g' ./pkg/models/mocks/transaction.go

pkg/models/mocks/record.go:
	mockgen -package=mocks -destination=pkg/models/mocks/record.go ${PATH_COURIER}/pkg/models Record
	$(SED) 's/github.com\/trussle\/courier\/vendor\///g' ./pkg/models/mocks/record.go

pkg/queue/mocks/queue.go:
	mockgen -package=mocks -destination=pkg/queue/mocks/queue.go ${PATH_COURIER}/pkg/queue Queue
	$(SED) 's/github.com\/trussle\/courier\/vendor\///g' ./pkg/queue/mocks/queue.go

.PHONY: build-mocks
build-mocks: FORCE
	$(MAKE) pkg/audit/mocks/log.go
	$(MAKE) pkg/metrics/mocks/metrics.go
	$(MAKE) pkg/metrics/mocks/observer.go
	$(MAKE) pkg/models/mocks/record.go
	$(MAKE) pkg/models/mocks/transaction.go
	$(MAKE) pkg/queue/mocks/queue.go

.PHONY: clean-mocks
clean-mocks: FORCE
	rm -f pkg/audit/mocks/log.go
	rm -f pkg/metrics/mocks/metrics.go
	rm -f pkg/metrics/mocks/observer.go
	rm -f pkg/models/mocks/record.go
	rm -f pkg/models/mocks/transaction.go
	rm -f pkg/queue/mocks/queue.go

.PHONY: clean
clean: FORCE
	rm -f dist/courier

FORCE:

.PHONY: unit-tests
unit-tests:
	docker-compose run courier go test -v ./cmd/... ./pkg/...

.PHONY: integration-tests
integration-tests:
	docker-compose run courier go test -v -tags=integration ./pkg/queue/...

.PHONY: documentation
documentation:
	go test -v -tags=documentation ./pkg/... -run=TestDocumentation_

.PHONY: coverage-tests
coverage-tests:
	docker-compose run courier go test -covermode=count -coverprofile=bin/coverage.out -v -tags=integration ${COVER_PKG}

.PHONY: coverage-view
coverage-view:
	go tool cover -html=bin/coverage.out

.PHONY: coverage
coverage:
	docker-compose run -e TRAVIS_BRANCH=${TRAVIS_BRANCH} -e GIT_BRANCH=${GIT_BRANCH} \
		courier \
		/bin/sh -c 'apk update && apk add make && apk add git && \
		go get github.com/mattn/goveralls && \
		/go/bin/goveralls -repotoken=${COVERALLS_REPO_TOKEN} -package=./pkg/... -flags=--tags=integration -service=travis-ci'

PWD ?= ${GOPATH}/src/${PATH_COURIER}
TAG ?= dev
BRANCH ?= dev
ifeq ($(BRANCH),master)
	TAG=latest
endif

.PHONY: build-docker
build-docker:
	@echo "Building '${TAG}' for '${BRANCH}'"
	docker run --rm -v ${PWD}:/go/src/${PATH_COURIER} -w /go/src/${PATH_COURIER} iron/go:dev go build -o courier ${PATH_COURIER}/cmd/courier
	docker build -t teamtrussle/courier:${TAG} .

.PHONY: push-docker-tag
push-docker-tag: FORCE
	@echo "Pushing '${TAG}' for '${BRANCH}'"
	docker login -u ${DOCKER_HUB_USERNAME} -p ${DOCKER_HUB_PASSWORD}
	docker push teamtrussle/courier:${TAG}

.PHONY: push-docker
ifeq ($(TAG),latest)
push-docker: FORCE
	@echo "Pushing '${TAG}' for '${BRANCH}'"
	docker login -u ${DOCKER_HUB_USERNAME} -p ${DOCKER_HUB_PASSWORD}
	docker push teamtrussle/courier:${TAG}
else
push-docker: FORCE
	@echo "Pushing requires branch '${BRANCH}' to be master"
endif
