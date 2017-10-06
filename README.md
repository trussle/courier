# Courier

![](assets/header.png)

Courier repository

## Badges

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
