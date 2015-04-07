package forestbucket

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"
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

func BenchmarkParallelAddGet(b *testing.B) {

	bucket, tempDir := GetTestBucket()
	defer func() {
		CloseBucket(bucket)
		os.RemoveAll(tempDir)
	}()

	counter := int64(0)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {

			nextKeyId := atomic.AddInt64(&counter, 1)
			key := fmt.Sprintf("key-%v", nextKeyId)
			added, err := bucket.Add(key, 0, []byte(`{"hello":"world"}`))
			if !added {
				log.Panicf("Unable to add key: %v", key)
			}
			if err != nil {
				log.Panicf("Error add key: %v. Err: %v", key, err)
			}
			var value interface{}
			err = bucket.Get(key, &value)
			if err != nil {
				log.Panicf("Error loading key: %v. Err: %v", key, err)
			}

		}
	})

}
