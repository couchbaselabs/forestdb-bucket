package forestbucket

import (
	"log"
	"os"
	"testing"
)

func BenchmarkOpenCloseBucket(b *testing.B) {

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bucket, tempDir := GetTestBucket()
			CloseBucket(bucket)
			os.RemoveAll(tempDir)

		}
	})

}

func BenchmarkGet(b *testing.B) {

	bucket, tempDir := GetTestBucket()
	defer func() {
		CloseBucket(bucket)
		os.RemoveAll(tempDir)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var value interface{}
		err := bucket.Get("key", &value)
		if err == nil {
			log.Panicf("No error calling bucket.Get(), expected error")
		}
	}

}

func BenchmarkParallelGet(b *testing.B) {

	bucket, tempDir := GetTestBucket()
	defer func() {
		CloseBucket(bucket)
		os.RemoveAll(tempDir)
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var value interface{}
			err := bucket.Get("key", &value)
			if err == nil {
				log.Panicf("No error calling bucket.Get(), expected error")
			}
		}
	})

}
