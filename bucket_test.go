package forestbucket

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestGetBucket(t *testing.T) {

	tempDir := os.TempDir()
	defer os.RemoveAll(tempDir)

	forestBucketUrl := fmt.Sprintf("forestdb:%v", tempDir)
	bucketName := "testbucket"

	// get a bucket
	bucket, err := GetBucket(
		forestBucketUrl,
		"default",
		bucketName,
	)
	assert.True(t, err == nil)
	assert.True(t, bucket != nil)

	// make sure it created the path for the bucket
	bucketPath := filepath.Join(tempDir, bucketName)
	_, err = os.Stat(bucketPath)
	assert.False(t, os.IsNotExist(err))

	// get the same bucket again, make sure it's the same
	bucketCopy, err := GetBucket(
		forestBucketUrl,
		"default",
		bucketName,
	)
	assert.True(t, err == nil)
	assert.Equals(t, bucket, bucketCopy)

	// get a bucket with an invalid url, assert error
	bucket, err = GetBucket(
		":invalid_url:",
		"default",
		"testbucket",
	)
	assert.True(t, err != nil)

	// get a bucket with no pool name
	bucket, err = GetBucket(
		forestBucketUrl,
		"",
		"testbucket",
	)
	assert.True(t, err == nil)
	assert.True(t, bucket != nil)

}
