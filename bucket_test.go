package forestbucket

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/walrus"
)

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

	CloseBucket(bucketCopy)
	log.Printf("closed bucket")

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

func GetTestBucket() (bucket walrus.Bucket, tempDir string) {
	// tempDir = os.TempDir()
	tempDir = "/tmp/foo"
	forestBucketUrl := fmt.Sprintf("forestdb:%v", tempDir)
	bucketName := "testbucket"

	bucket, err := GetBucket(
		forestBucketUrl,
		DefaultPoolName,
		bucketName,
	)

	if err != nil {
		log.Panicf("Error creating bucket: %v", err)
	}

	return bucket, tempDir

}

func TestDeleteThenAdd(t *testing.T) {

	tempDir := os.TempDir()
	defer os.RemoveAll(tempDir)
	forestBucketUrl := fmt.Sprintf("forestdb:%v", tempDir)
	bucketName := "testbucket"

	bucket, err := GetBucket(
		forestBucketUrl,
		DefaultPoolName,
		bucketName,
	)
	assert.True(t, err == nil)
	defer bucket.Close()

	var value interface{}
	err = bucket.Get("key", &value)
	assert.True(t, err != nil)

	/*

	   TODO: re-enable

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
	*/

}

func assertNoError(t *testing.T, err error, message string) {
	if err != nil {
		t.Fatalf("%s: %v", message, err)
	}
}
