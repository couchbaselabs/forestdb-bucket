package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/couchbaselabs/forestdb-bucket"
)

// For whatever reason, the following does not seem to yield any memory profiling results:
//
// $ go test -bench BenchmarkView -run XXX -memprofile /tmp/memprofile -benchmem
// $ go tool pprof forestdb-bucket.test /tmp/memprofile
//
// As a workaround, I'm running the same code in normal binary with net/http/pprof
// imported, so that it provides the http endpoints for viewing profiling data

func main() {

	go func() {

		bucket, tempDir := forestbucket.GetTestBucket()
		defer func() {
			forestbucket.CloseBucket(bucket)
			os.RemoveAll(tempDir)
		}()

		forestbucket.AddTestDesignDoc(bucket)

		for i := 0; i < 100000; i++ {
			forestbucket.AddTestDocQueryView(bucket, i)
		}

		log.Println("Done")

	}()

	log.Println(http.ListenAndServe("localhost:6060", nil))
}
