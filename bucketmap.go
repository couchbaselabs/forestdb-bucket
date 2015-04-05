package forestbucket

import (
	"fmt"

	"github.com/couchbaselabs/walrus"
	"github.com/tleyden/go-safe-dstruct/mapserver"
)

type bucketMap struct {
	wrapped mapserver.MapServer
}

func NewBucketMap() *bucketMap {
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
