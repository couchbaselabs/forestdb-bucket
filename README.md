
[![Join the chat at https://gitter.im/couchbase/mobile](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/couchbase/mobile?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A ForestDB bucket for Sync Gateway that conforms to the Walrus Bucket interface.

You will first need to install ForestDB (see goforestdb README)

## Documentation

[![GoDoc](https://godoc.org/github.com/tleyden/forestdb-bucket?status.png)](https://godoc.org/github.com/tleyden/forestdb-bucket) 

## Running tests

```
$ go test -v -race -cpu 4
```

## Running benchmarks


```
$ go test -v -bench . -cpu 4
```
