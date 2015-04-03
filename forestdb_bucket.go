package forestbucket

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/couchbaselabs/walrus"
)

type forestdbBucket struct {
	name           string // Name of the bucket
	bucketRootPath string // Filesystem path where all the bucket dirs live
	poolName       string // Name of the pool
}

// Creates a new ForestDB bucket
func NewBucket(dir, poolName, bucketName string) (walrus.Bucket, error) {

	bucket := &forestdbBucket{
		name:           bucketName,
		bucketRootPath: dir,
		poolName:       poolName,
	}
	runtime.SetFinalizer(bucket, (*forestdbBucket).Close)

	// create bucket path if needed
	if err := os.MkdirAll(bucket.bucketPath(), 0777); err != nil {
		return nil, err
	}

	return bucket, nil
}

// Get the name of the directory where the forestdb files will be stored,
// eg: "mybucket" or "poolname-mybucket"
func (b forestdbBucket) bucketDirName() string {
	switch b.poolName {
	case "":
		return b.name
	case "default":
		return b.name
	default:
		return fmt.Sprintf("%v-%v", b.poolName, b.name)
	}
}

// Get the path to the bucket, eg: /var/lib/buckets/mybucket
func (b forestdbBucket) bucketPath() string {
	return filepath.Join(
		b.bucketRootPath,
		b.bucketDirName(),
	)
}

////////////////////////////////////////////////////////////////////////////////////

// walrus.Bucket interface methods

func (b *forestdbBucket) GetName() string {
	return ""
}

func (b *forestdbBucket) Get(k string, rv interface{}) error {
	return nil
}

func (b *forestdbBucket) GetRaw(k string) ([]byte, error) {
	return nil, nil
}

func (b *forestdbBucket) Add(k string, exp int, v interface{}) (added bool, err error) {
	return false, nil
}

func (b *forestdbBucket) AddRaw(k string, exp int, v []byte) (added bool, err error) {
	return false, nil
}

func (b *forestdbBucket) Append(k string, data []byte) error {
	return nil
}

func (b *forestdbBucket) Set(k string, exp int, v interface{}) error {
	return nil
}

func (b *forestdbBucket) SetRaw(k string, exp int, v []byte) error {
	return nil
}

func (b *forestdbBucket) Delete(k string) error {
	return nil
}

func (b *forestdbBucket) Write(k string, flags int, exp int, v interface{}, opt walrus.WriteOptions) error {
	return nil
}

func (b *forestdbBucket) Update(k string, exp int, callback walrus.UpdateFunc) error {
	return nil
}

func (b *forestdbBucket) WriteUpdate(k string, exp int, callback walrus.WriteUpdateFunc) error {
	return nil
}

func (b *forestdbBucket) Incr(k string, amt, def uint64, exp int) (uint64, error) {
	return 0, nil
}

func (b *forestdbBucket) GetDDoc(docname string, into interface{}) error {
	return nil
}

func (b *forestdbBucket) PutDDoc(docname string, value interface{}) error {
	return nil
}

func (b *forestdbBucket) DeleteDDoc(docname string) error {
	return nil
}

func (b *forestdbBucket) View(ddoc, name string, params map[string]interface{}) (walrus.ViewResult, error) {
	return walrus.ViewResult{}, nil
}

func (b *forestdbBucket) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	return nil
}

func (b *forestdbBucket) StartTapFeed(args walrus.TapArguments) (walrus.TapFeed, error) {
	return nil, nil
}

func (b *forestdbBucket) Close() {

}

func (b *forestdbBucket) Dump() {

}

func (b *forestdbBucket) VBHash(docID string) uint32 {
	return 0
}

// End walrus.Bucket interface methods

////////////////////////////////////////////////////////////////////////////////////
