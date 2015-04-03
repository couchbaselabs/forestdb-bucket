package forestbucket

import "github.com/couchbaselabs/walrus"

func init() {
	buckets = NewBucketMap()
}

// Keep track of all buckets that have been opened
var buckets *bucketMap

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

	key := buckets.key(url, poolName, bucketName)
	bucket, found := buckets.find(key)

	if !found {
		dir, err := bucketURLToDir(url)
		if err != nil {
			return nil, err
		}
		bucket, err = NewBucket(dir, poolName, bucketName)
		if err != nil {
			return nil, err
		}

		buckets.insert(key, bucket)
	}
	return bucket, nil

}
