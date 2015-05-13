package forestbucket

import (
	"fmt"

	"github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/go-safe-dstruct/mapserver"
)

type bucketMap struct {
	wrapped mapserver.MapServer
}

func newBucketMap() *bucketMap {
	bucketMap := &bucketMap{
		wrapped: mapserver.NewMapserver(),
	}
	return bucketMap
}

func (bm bucketMap) key(url, poolName, bucketName string) string {

	if poolName == "" {
		poolName = "default"
	}

	return fmt.Sprintf(
		"%v-%v-%v",
		url,
		poolName,
		bucketName,
	)

}

func (bm bucketMap) find(key string) (bucket sgbucket.Bucket, found bool) {

	rawVal, found := bm.wrapped.Find(key)
	if !found {
		return nil, found
	}
	return rawVal.(sgbucket.Bucket), true

}

func (bm bucketMap) insert(key string, bucket sgbucket.Bucket) {
	bm.wrapped.Insert(key, bucket)
}

func (bm bucketMap) delete(key string) {
	bm.wrapped.Delete(key)
}

// Snapshot returns a snapshot copy of the map
func (bm bucketMap) snapshot() map[string]sgbucket.Bucket {
	reply := make(map[string]sgbucket.Bucket)
	interfaceSnapshot := bm.wrapped.Snapshot()
	for k, v := range interfaceSnapshot {
		reply[k] = v.(sgbucket.Bucket)
	}
	return reply
}
