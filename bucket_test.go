package forestbucket

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/goforestdb"
	"github.com/couchbaselabs/walrus"
)

func TestDurableAdd(t *testing.T) {

	key := "key"
	val := map[string]string{
		"foo": "bar",
	}

	// get a test bucket
	bucket, tempDir := GetTestBucket()

	// clean up dir when we're done
	defer os.RemoveAll(tempDir)

	// get the file path
	fdbBucket := bucket.(*forestdbBucket)
	dbFile := fdbBucket.bucketDbFilePath()

	// add a key
	added, err := bucket.Add(key, 0, val)
	assertNoError(t, err, "Add")
	assert.True(t, added)

	// close the bucket
	CloseBucket(bucket)

	// open forestdb database file + kvstore
	db, err := forestdb.Open(dbFile, nil)
	assert.True(t, err == nil)
	kvstore, err := db.OpenKVStoreDefault(nil)
	assert.True(t, err == nil)

	// make sure we can get the doc by key and it has expected val
	doc, err := forestdb.NewDoc([]byte(key), nil, nil)
	assert.True(t, err == nil)
	defer doc.Close()
	err = kvstore.Get(doc)
	assert.True(t, err == nil)
	assert.True(t, len(doc.Body()) > 0)
	docJson := make(map[string]string)
	err = json.Unmarshal(doc.Body(), &docJson)
	assert.True(t, err == nil)
	assert.Equals(t, docJson["foo"], "bar")

}

func TestDeleteThenAdd(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	var value interface{}
	err := bucket.Get("key", &value)
	assert.True(t, err != nil)

	added, err := bucket.Add("key", 0, "value")
	assertNoError(t, err, "Add")
	assert.True(t, added)
	assertNoError(t, bucket.Get("key", &value), "Get")
	assert.Equals(t, value, "value")

	assertNoError(t, bucket.Delete("key"), "Delete")
	err = bucket.Get("key", &value)
	assert.True(t, err != nil)
	added, err = bucket.Add("key", 0, "value")
	assertNoError(t, err, "Add")
	assert.True(t, added)

}

func TestIncr(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	count, err := bucket.Incr("count1", 1, 100, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, count, uint64(100))

	count, err = bucket.Incr("count1", 0, 0, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, count, uint64(100))

	count, err = bucket.Incr("count1", 10, 100, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, count, uint64(110))

	count, err = bucket.Incr("count1", 0, 0, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, count, uint64(110))

	count, err = bucket.Incr("count2", 0, 0, -1)
	assertTrue(t, err != nil, "Expected error from Incr")

}

// Spawns 1000 goroutines that 'simultaneously' use Incr to increment the same counter by 1.
func TestIncrAtomic(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	var waiters sync.WaitGroup
	numIncrements := 1000
	waiters.Add(numIncrements)
	for i := uint64(1); i <= uint64(numIncrements); i++ {
		numToAdd := i // lock down the value for the goroutine
		go func() {
			_, err := bucket.Incr("key", numToAdd, numToAdd, 0)
			assertNoError(t, err, "Incr")
			waiters.Add(-1)
		}()
	}
	waiters.Wait()
	value, err := bucket.Incr("key", 0, 0, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, int(value), numIncrements*(numIncrements+1)/2)
}

func TestAppend(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	err := bucket.Append("key", []byte(" World"))
	assert.DeepEquals(t, err, walrus.MissingError{
		Key: "key",
	})

	log.Printf("call SetRaw")
	err = bucket.SetRaw("key", 0, []byte("Hello"))
	assertNoError(t, err, "SetRaw")
	err = bucket.Append("key", []byte(" World"))
	assertNoError(t, err, "Append")
	value, err := bucket.GetRaw("key")
	assertNoError(t, err, "GetRaw")
	assert.DeepEquals(t, value, []byte("Hello World"))
}

func TestGetBucket(t *testing.T) {

	tempDir := os.TempDir()
	defer os.RemoveAll(tempDir)

	forestBucketUrl := fmt.Sprintf("forestdb:%v", tempDir)
	bucketName := "testbucket"

	// get a bucket
	bucket, err := GetBucket(
		forestBucketUrl,
		DefaultPoolName,
		bucketName,
	)
	defer CloseBucket(bucket)

	assert.True(t, err == nil)
	assert.True(t, bucket != nil)
	assert.Equals(t, bucket.GetName(), bucketName)

	// make sure it created the path for the bucket
	bucketPath := filepath.Join(tempDir, bucketName)
	_, err = os.Stat(bucketPath)
	assert.False(t, os.IsNotExist(err))

	// get the same bucket again, make sure it's the same
	bucketCopy, err := GetBucket(
		forestBucketUrl,
		DefaultPoolName,
		bucketName,
	)
	assert.True(t, err == nil)
	assert.Equals(t, bucket, bucketCopy)

}

func TestGetInvalidBucket(t *testing.T) {

	// get a bucket with an invalid url, assert error
	_, err := GetBucket(
		":invalid_url:",
		DefaultPoolName,
		"testbucket",
	)
	assert.True(t, err != nil)

}

func TestGetBucketNoPoolName(t *testing.T) {

	tempDir := os.TempDir()
	defer os.RemoveAll(tempDir)

	forestBucketUrl := fmt.Sprintf("forestdb:%v", tempDir)

	// Get a bucket with no pool name
	bucket, err := GetBucket(
		forestBucketUrl,
		"",
		"testbucket",
	)
	assert.True(t, err == nil)
	assert.True(t, bucket != nil)

}
