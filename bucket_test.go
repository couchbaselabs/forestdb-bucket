package forestbucket

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

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

	/*

	   TODO: re-enable

	   	assertNoError(t, bucket.Delete("key"), "Delete")
	   	err = bucket.Get("key", &value)
	   	assert.True(t, err != nil)
	   	added, err = bucket.Add("key", 0, "value")
	   	assertNoError(t, err, "Add")
	   	assert.True(t, added)
	*/

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
