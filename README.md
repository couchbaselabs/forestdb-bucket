
[![Join the chat at https://gitter.im/couchbase/mobile](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/couchbase/mobile?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A ForestDB bucket for Sync Gateway that conforms to the Walrus Bucket interface.

You will first need to install ForestDB (see goforestdb README)

## Documentation

[![GoDoc](https://godoc.org/github.com/couchbaselabs/forestdb-bucket?status.png)](https://godoc.org/github.com/couchbaselabs/forestdb-bucket) 

## Running tests

```
$ go test -v -race -cpu 4
```

## Running benchmarks


```
$ go test -v -bench . -cpu 4
```

## Generate code coverage report

```
$ go test -coverprofile=coverage.out 
$ go tool cover -html=coverage.out
```

## TODO

* Views are not being created on startup
  * Test: create view, close and re-open bucket, query view
* Views are stored in memory
  * Add benchmarks that show this is slow (re-open db and query a view, which will cause rebuild)
* Views are walking the entire set of docs instead of starting at a startSeq
  * Add benchmarks that add lots of docs, show how views don't scale
* No database compaction is happening

