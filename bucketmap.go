package forestbucket

import (
	"fmt"

	"github.com/couchbaselabs/go-safe-dstruct/mapserver"
	"github.com/couchbaselabs/walrus"
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

func (bm bucketMap) find(key string) (bucket walrus.Bucket, found bool) {

	rawVal, found := bm.wrapped.Find(key)
	if !found {
		return nil, found
	}
	return rawVal.(walrus.Bucket), true

}

func (bm bucketMap) insert(key string, bucket walrus.Bucket) {
	bm.wrapped.Insert(key, bucket)
}

func (bm bucketMap) delete(key string) {
	bm.wrapped.Delete(key)
}

// Snapshot returns a snapshot copy of the map
func (bm bucketMap) snapshot() map[string]walrus.Bucket {
	reply := make(map[string]walrus.Bucket)
	interfaceSnapshot := bm.wrapped.Snapshot()
	for k, v := range interfaceSnapshot {
		reply[k] = v.(walrus.Bucket)
	}
	return reply
}
