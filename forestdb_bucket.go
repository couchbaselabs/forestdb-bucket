package forestbucket

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/couchbaselabs/goforestdb"
	"github.com/couchbaselabs/walrus"
)

type forestdbBucket struct {
	name           string            // Name of the bucket
	bucketRootPath string            // Filesystem path where all the bucket dirs live
	poolName       string            // Name of the pool
	db             *forestdb.File    // The forestdb db handle
	kvstore        *forestdb.KVStore // The forestdb key value store
	lock           sync.RWMutex      // For thread-safety
}

// Creates a new ForestDB bucket
func NewBucket(bucketRootPath, poolName, bucketName string) (walrus.Bucket, error) {

	bucket := &forestdbBucket{
		name:           bucketName,
		bucketRootPath: bucketRootPath,
		poolName:       poolName,
	}

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
func (b *forestdbBucket) bucketDirName() string {
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
func (b *forestdbBucket) bucketPath() string {
	return filepath.Join(
		b.bucketRootPath,
		b.bucketDirName(),
	)
}

// Get the full path to the db file, eg, /var/lib/buckets/mybucket/mybucket.fdb
func (b *forestdbBucket) bucketDbFilePath() string {
	return filepath.Join(
		b.bucketPath(),
		fmt.Sprintf("%v.fdb", b.name),
	)
}

////////////////////////////////////////////////////////////////////////////////////

// walrus.Bucket interface methods

func (bucket *forestdbBucket) GetName() string {
	return bucket.name
}

func (bucket *forestdbBucket) Get(key string, returnVal interface{}) error {

	// Lookup the document
	doc, err := forestdb.NewDoc([]byte(key), nil, nil)
	if err != nil {
		return err
	}
	defer doc.Close()
	if err := bucket.kvstore.Get(doc); err != nil {
		return err
	}
	if !doc.Deleted() {
		return json.Unmarshal(doc.Body(), returnVal)
	}
	return nil
}

func (bucket *forestdbBucket) GetRaw(key string) ([]byte, error) {
	return nil, nil
}

func (bucket *forestdbBucket) Add(key string, expires int, value interface{}) (added bool, err error) {

	// Marshal JSON
	data, err := json.Marshal(value)
	if err != nil {
		return false, err
	}

	return bucket.AddRaw(
		key,
		expires,
		data,
	)

}

func (bucket *forestdbBucket) AddRaw(key string, expires int, value []byte) (added bool, err error) {

	// if doc already exists, throw an error
	exists, err := bucket.keyExists(key)
	if err != nil {
		return false, err
	}
	if exists {
		return false, walrus.ErrKeyExists
	}

	return bucket.addRaw(
		key,
		expires,
		value,
	)
}

func (bucket *forestdbBucket) addRaw(key string, expires int, value []byte) (added bool, err error) {

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	doc, err := forestdb.NewDoc([]byte(key), nil, value)
	if err != nil {
		return false, err
	}
	defer doc.Close()
	if err := bucket.kvstore.Set(doc); err != nil {
		return false, err
	}
	return true, nil

}

func (bucket *forestdbBucket) Append(key string, data []byte) error {
	return nil
}

// Set key to value with expires time (which is ignored)
func (bucket *forestdbBucket) Set(key string, expires int, value interface{}) error {

	return nil
}

func (bucket *forestdbBucket) SetRaw(key string, expires int, v []byte) error {
	return nil
}

func (bucket *forestdbBucket) Delete(key string) error {
	return nil
}

func (bucket *forestdbBucket) Write(key string, flags int, expires int, value interface{}, opt walrus.WriteOptions) error {
	return nil
}

func (bucket *forestdbBucket) Update(key string, expires int, callback walrus.UpdateFunc) error {
	return nil
}

func (bucket *forestdbBucket) WriteUpdate(key string, expires int, callback walrus.WriteUpdateFunc) error {
	return nil
}

func (bucket *forestdbBucket) Incr(key string, amt, defaultVal uint64, expires int) (uint64, error) {
	return 0, nil
}

func (bucket *forestdbBucket) GetDDoc(docname string, into interface{}) error {
	return nil
}

func (bucket *forestdbBucket) PutDDoc(docname string, value interface{}) error {
	return nil
}

func (bucket *forestdbBucket) DeleteDDoc(docname string) error {
	return nil
}

func (bucket *forestdbBucket) View(ddoc, name string, params map[string]interface{}) (walrus.ViewResult, error) {
	return walrus.ViewResult{}, nil
}

func (bucket *forestdbBucket) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	return nil
}

func (bucket *forestdbBucket) StartTapFeed(args walrus.TapArguments) (walrus.TapFeed, error) {
	return nil, nil
}

func (bucket *forestdbBucket) Close() {
	bucket.kvstore.Close()
	bucket.db.Close()
}

func (bucket *forestdbBucket) Dump() {

}

func (bucket *forestdbBucket) VBHash(docID string) uint32 {
	return 0
}

// End walrus.Bucket interface methods

////////////////////////////////////////////////////////////////////////////////////

func (bucket *forestdbBucket) keyExists(key string) (exists bool, err error) {

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	// Lookup the document
	doc, err := forestdb.NewDoc([]byte(key), nil, nil)
	if err != nil {
		return false, err
	}
	defer doc.Close()
	err = bucket.kvstore.Get(doc)

	if err != nil {
		if err == forestdb.RESULT_KEY_NOT_FOUND {
			return false, nil
		} else {
			return false, err
		}
	}

	if doc.Deleted() {
		return false, nil
	}

	// if the doc has a body, then it exists
	exists = (len(doc.Body()) > 0)

	return exists, nil

}
