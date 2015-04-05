package forestbucket

import (
	"log"

	"github.com/couchbaselabs/walrus"
)

const (
	DefaultPoolName = "default"
)

// Keep track of all buckets that have been opened
var buckets *bucketMap

func init() {
	buckets = NewBucketMap()
}

// Returns a ForestDB-backed Bucket specific to the given (url, pool, bucketname) tuple.
// That is, passing the same parameters will return the same Bucket.
//
// If the urlStr should have an absolute or relative path:
//
//		forestdb:/foo/bar
//		forestdb:bar
//
// The bucket's directory will live inside the given directory in the path and
// will be named "bucketName", or if the poolName is not
// "default", "poolName-bucketName".
//
func GetBucket(url, poolName, bucketName string) (walrus.Bucket, error) {

	bucketRootPath, err := bucketURLToDir(url)
	if err != nil {
		return nil, err
	}

	key := buckets.key(bucketRootPath, poolName, bucketName)
	bucket, found := buckets.find(key)

	if !found {
		bucket, err = NewBucket(bucketRootPath, poolName, bucketName)
		if err != nil {
			return nil, err
		}
		log.Printf("created new bucket: %v", bucket)

		buckets.insert(key, bucket)
	} else {
		log.Printf("return existig bucket: %v", bucket)
	}
	return bucket, nil

}

// Close a bucket and remove from cache of Bucket objects
func CloseBucket(bucket walrus.Bucket) {

	forestdbBucket := bucket.(*forestdbBucket)
	key := buckets.key(
		forestdbBucket.bucketRootPath,
		forestdbBucket.poolName,
		forestdbBucket.name,
	)

	bucket.Close()
	buckets.delete(key)

}
