# Courier

![](assets/header.png)

Courier repository

## Badges

[![travis-ci](https://travis-ci.org/trussle/courier.svg?branch=master)](https://travis-ci.org/trussle/courier)
[![Coverage Status](https://coveralls.io/repos/github/trussle/courier/badge.svg?branch=master)](https://coveralls.io/github/trussle/courier?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/trussle/courier)](https://goreportcard.com/report/github.com/trussle/courier)

## Introduction

Courier is a SQS message ingester that egresses individual messages to a http
client. Each message will be pulled of the queue (in batches or individually,
depending on configuration) and then stored and batched before sending to 
downstream clients.

This does not provide a full audit trail of every message, rather it audit's 
every successful message that has been transacted through the pipeline.

Consider the following:

```
                                                                                +--------------+
                                                                                |              |
                                                                            +--->  Client N+1  |
                          +----------------+                                |   |              |
                          |                |                                |   +--------------+
+-----------------+   +--->  Consumer N+1  +-----+                          |
|                 +---+   |                |     |     +-----------------+  |   +--------------+
|  SQS Messaging  |       +----------------+     |     |                 |  |   |              |
|      Queue      |                              +--+-->  Load Balancer  +------>  Client N+1  |
|                 +---+   +----------------+     |  |  |                 |  |   |              |
+-----------------+   |   |                |     |  |  +-----------------+  |   +--------------+
                      +--->  Consumer N+1  +-----+  |                       |
                          |                |        |                       |   +--------------+
                          +----------------+        +-----------+           |   |              |
                                                                |           +--->  Client N+1  |
                                                                |               |              |
                                                                |               +--------------+
                                                      +---------v---------+
                                                      |                   |
                                                      |  Auditing Output  |
                                                      |    Firehose/FS    |
                                                      |                   |
                                                      +-------------------+
```

It expects that you have some sort of load balancing system in places, as it's
not hard to run tens of thousands of messages from a queue towards the http 
client, so some thought should be provided as such.

Rate-limiting of messages can be controlled by reducing the amount of consumers
for now, but in the future it would be easy to envision a configurable component
to do so.


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
