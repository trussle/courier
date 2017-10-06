# Courier

![](assets/header.png)

Courier repository

## Badges

[![travis-ci](https://travis-ci.org/trussle/courier.svg?branch=master)](https://travis-ci.org/trussle/courier)
[![Coverage Status](https://coveralls.io/repos/github/trussle/courier/badge.svg?branch=master)](https://coveralls.io/github/trussle/courier?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/trussle/courier)](https://goreportcard.com/report/github.com/trussle/courier)

## Introduction

## Setup

### Local development

Courier expects that you have a `$GOPATH` configured correctly along with 
`$GOPATH/bin` in your `$PATH`. Once these are setup, it should be as simple as 
make install, which will get all the correct dependencies for you to be able to 
start working with the code.

### Integration development

Integration development (testing) requires both docker and docker-compose to be 
installed. Running the following, should create the right dependencies for the 
integration tests to run against:

```
docker-compose up --build -d
make integration-tests
```

## API Endpoints

The following contains the documentation for the API end points for Courier.
