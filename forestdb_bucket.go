package forestbucket

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/couchbaselabs/goforestdb"
	"github.com/couchbaselabs/walrus"
)

type forestdbBucket struct {
	name           string            // Name of the bucket
	bucketRootPath string            // Filesystem path where all the bucket dirs live
	poolName       string            // Name of the pool
	db             *forestdb.File    // The forestdb db handle
	kvstore        *forestdb.KVStore // The forestdb key value store
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

	// open forestdb database file
	db, err := forestdb.Open(bucket.bucketDbFilePath(), nil)
	if err != nil {
		return nil, err
	}
	bucket.db = db

	// open forestdb kvstore
	kvstore, err := db.OpenKVStoreDefault(nil)
	if err != nil {
		return nil, err
	}
	bucket.kvstore = kvstore

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

// Get the full path to the db file, eg, /var/lib/buckets/mybucket/mybucket.fdb
func (b forestdbBucket) bucketDbFilePath() string {
	return filepath.Join(
		b.bucketPath(),
		fmt.Sprintf("%v.fdb", b.name),
	)
}

////////////////////////////////////////////////////////////////////////////////////

// walrus.Bucket interface methods

func (b *forestdbBucket) GetName() string {
	return b.name
}

func (b *forestdbBucket) Get(key string, returnVal interface{}) error {
	// Lookup the document
	doc, err := forestdb.NewDoc([]byte(key), nil, nil)
	if err != nil {
		return err
	}
	defer doc.Close()
	if err := b.kvstore.Get(doc); err != nil {
		return err
	}
	returnVal = doc
	return nil
}

func (b *forestdbBucket) GetRaw(key string) ([]byte, error) {
	return nil, nil
}

func (b *forestdbBucket) Add(key string, expires int, value interface{}) (added bool, err error) {
	return false, nil
}

func (b *forestdbBucket) AddRaw(key string, expires int, v []byte) (added bool, err error) {
	return false, nil
}

func (b *forestdbBucket) Append(key string, data []byte) error {
	return nil
}

// Set key to value with expires time (which is ignored)
func (b *forestdbBucket) Set(key string, expires int, value interface{}) error {
	return nil
}

func (b *forestdbBucket) SetRaw(key string, expires int, v []byte) error {
	return nil
}

func (b *forestdbBucket) Delete(key string) error {
	return nil
}

func (b *forestdbBucket) Write(key string, flags int, expires int, value interface{}, opt walrus.WriteOptions) error {
	return nil
}

func (b *forestdbBucket) Update(key string, expires int, callback walrus.UpdateFunc) error {
	return nil
}

func (b *forestdbBucket) WriteUpdate(key string, expires int, callback walrus.WriteUpdateFunc) error {
	return nil
}

func (b *forestdbBucket) Incr(key string, amt, defaultVal uint64, expires int) (uint64, error) {
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
	b.kvstore.Close()
	b.db.Close()
}

func (b *forestdbBucket) Dump() {

}

func (b *forestdbBucket) VBHash(docID string) uint32 {
	return 0
}

// End walrus.Bucket interface methods

////////////////////////////////////////////////////////////////////////////////////
