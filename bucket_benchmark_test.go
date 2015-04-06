package forestbucket

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func TestOpenCloseBucket(t *testing.T) {
	for i := 0; i < 5; i++ {
		log.Printf("call GetTestBucket")
		bucket, tempDir := GetTestBucket()
		log.Printf("benchmark bucket %v in %v", bucket, tempDir)
		CloseBucket(bucket)
		log.Printf("closed bucket %v in %v", bucket, tempDir)
		os.RemoveAll(tempDir)
		log.Printf("os.removeall")
	}

}

func BenchmarkOpenCloseBucket(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bucket, tempDir := GetTestBucket()
			CloseBucket(bucket)
			os.RemoveAll(tempDir)

		}
	})

	/*
		for i := 0; i < b.N; i++ {
			bucket, tempDir := GetTestBucket()
			CloseBucket(bucket)
			os.RemoveAll(tempDir)
		}
	*/

}

func BenchmarkGet(b *testing.B) {
	bucket, tempDir := GetTestBucket()
	log.Printf("benchmark bucket %v in %v", bucket, tempDir)
	defer func() {
		CloseBucket(bucket)
		os.RemoveAll(tempDir)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// bucket.Get()
		fmt.Sprintf("hello")
	}
}
