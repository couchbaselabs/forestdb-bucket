package forestbucket

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/couchbaselabs/goforestdb"
	"github.com/couchbaselabs/walrus"
)

type forestdbBucket struct {
	name           string                       // Name of the bucket
	bucketRootPath string                       // Filesystem path where all the bucket dirs live
	poolName       string                       // Name of the pool
	db             *forestdb.File               // The forestdb db handle
	kvstore        *forestdb.KVStore            // The primary forestdb key value store
	kvstoreDdocs   *forestdb.KVStore            // Another kv store for design docs
	lock           sync.RWMutex                 // For thread-safety
	tapFeeds       []*tapFeedImpl               // Tap feeds
	views          map[string]forestdbDesignDoc // Stores runtime view/index data
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

	// open primary kvstore
	kvstore, err := db.OpenKVStoreDefault(nil)
	if err != nil {
		return nil, err
	}
	bucket.kvstore = kvstore

	// open design docs kvstore
	kvstoreDdocs, err := db.OpenKVStore("designdocs", nil)
	if err != nil {
		return nil, err
	}
	bucket.kvstoreDdocs = kvstoreDdocs

	// create a new map for holding instantiated views
	bucket.views = map[string]forestdbDesignDoc{}

	/*

		        TODO: need to port this from lolrus after writing a unit test

			// Recompile the design docs:
			for name, ddoc := range bucket.DesignDocs {
				if err := bucket._compileDesignDoc(name, ddoc); err != nil {
					return nil, err
				}
			}

	*/

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

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

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

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	// Lookup the document
	doc, err := forestdb.NewDoc([]byte(key), nil, nil)
	if err != nil {
		return nil, err
	}
	defer doc.Close()
	if err := bucket.kvstore.Get(doc); err != nil {
		return nil, err
	}
	if !doc.Deleted() {
		return doc.Body(), nil
	}
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

	if err := bucket.setRaw(
		key,
		expires,
		value,
	); err != nil {
		return false, err
	}
	return true, nil

}

func (bucket *forestdbBucket) setRaw(key string, expires int, value []byte) error {

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	doc, err := forestdb.NewDoc([]byte(key), nil, value)
	if err != nil {
		return err
	}
	defer doc.Close()
	if err := bucket.kvstore.Set(doc); err != nil {
		return err
	}

	if err := bucket.db.Commit(forestdb.COMMIT_NORMAL); err != nil {
		return err
	}

	// Post a TAP notification:
	bucket._postTapMutationEvent(key, value, uint64(doc.SeqNum()))

	return nil

}

func (bucket *forestdbBucket) Append(key string, data []byte) error {

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	// lookup doc for key
	doc, err := forestdb.NewDoc([]byte(key), nil, nil)
	if err != nil {
		log.Printf("error creating doc: %v", err)
		return err
	}
	defer doc.Close()

	err = bucket.kvstore.Get(doc)
	if err != nil {
		if err == forestdb.RESULT_KEY_NOT_FOUND {
			return walrus.MissingError{
				Key: key,
			}
		}
		return err
	}

	// if it does exist, append this data onto it
	updatedDoc, err := forestdb.NewDoc(
		[]byte(key),
		nil,
		append(doc.Body(), data...),
	)
	if err != nil {
		return err
	}
	if err := bucket.kvstore.Set(updatedDoc); err != nil {
		return err
	}
	if err := bucket.db.Commit(forestdb.COMMIT_NORMAL); err != nil {
		return err
	}

	bucket._postTapMutationEvent(key, updatedDoc.Body(), uint64(updatedDoc.SeqNum()))

	return nil
}

// Set key to value with expires time (which is ignored)
func (bucket *forestdbBucket) Set(key string, expires int, value interface{}) error {

	// Marshal JSON
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return bucket.SetRaw(key, expires, data)

}

func (bucket *forestdbBucket) SetRaw(key string, expires int, value []byte) error {
	return bucket.setRaw(
		key,
		expires,
		value,
	)
}

func (bucket *forestdbBucket) Delete(key string) error {

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	doc, err := forestdb.NewDoc([]byte(key), nil, nil)
	if err != nil {
		return err
	}
	defer doc.Close()
	if err := bucket.kvstore.Set(doc); err != nil {
		return err
	}

	// Post a TAP notification:
	bucket._postTapDeletionEvent(key, uint64(doc.SeqNum()))

	return nil

}

func (bucket *forestdbBucket) Write(key string, flags int, expires int, value interface{}, opt walrus.WriteOptions) error {
	log.Panicf("Write not implemented")
	return nil
}

func (bucket *forestdbBucket) Update(key string, expires int, callback walrus.UpdateFunc) error {

	writeCallback := func(current []byte) (updated []byte, opts walrus.WriteOptions, err error) {
		updated, err = callback(current)
		return
	}
	return bucket.WriteUpdate(key, expires, writeCallback)
}

func (bucket *forestdbBucket) WriteUpdate(key string, expires int, callback walrus.WriteUpdateFunc) error {

	// can't lock here, because setRaw locks below ..
	// bucket.lock.Lock()
	// defer bucket.lock.Unlock()

	docBody, err := bucket.kvstore.GetKV([]byte(key))
	if err != nil && err != forestdb.RESULT_KEY_NOT_FOUND {
		// if it's an unexpected error, return it
		return err
	}

	// TODO: is copySlice really needed here?
	newDocBody, _, err := callback(copySlice(docBody))
	if err != nil {
		return err
	}

	if err := bucket.setRaw(key, expires, newDocBody); err != nil {
		return err
	}

	return nil
}

func (bucket *forestdbBucket) Incr(key string, amt, defaultVal uint64, expires int) (uint64, error) {

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	// Lookup the document
	doc, err := forestdb.NewDoc([]byte(key), nil, nil)
	if err != nil {
		return 0, err
	}
	defer doc.Close()

	// get the dock from the kvstore
	err = bucket.kvstore.Get(doc)

	// we expect no error, or a key not found error if it doesn't exist
	if err != nil && err != forestdb.RESULT_KEY_NOT_FOUND {
		return 0, err
	}

	counter := defaultVal
	if doc.Body() != nil && len(doc.Body()) > 0 {
		counter, err = strconv.ParseUint(string(doc.Body()), 10, 64)
		if err != nil {
			return 0, err
		}
		if amt == 0 {
			return counter, nil // just reading existing value
		}
		counter += amt
	} else {
		if expires < 0 {
			return 0, walrus.MissingError{
				Key: key,
			}
		}
		counter = defaultVal
	}

	// update the doc with incremented value
	updatedDoc, err := forestdb.NewDoc(
		[]byte(key),
		nil,
		[]byte(strconv.FormatUint(counter, 10)),
	)
	if err != nil {
		return 0, err
	}
	if err := bucket.kvstore.Set(updatedDoc); err != nil {
		return 0, err
	}
	if err := bucket.db.Commit(forestdb.COMMIT_NORMAL); err != nil {
		return 0, err
	}

	bucket._postTapMutationEvent(key, updatedDoc.Body(), uint64(updatedDoc.SeqNum()))

	return counter, nil

}

func (bucket *forestdbBucket) Close() {
	bucket.kvstore.Close()
	bucket.db.Close()
}

func (bucket *forestdbBucket) Dump() {
	log.Panicf("Dump() not implemented")
}

func (bucket *forestdbBucket) VBHash(docID string) uint32 {
	log.Panicf("VBHash() not implemented")
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

func (bucket *forestdbBucket) LastSeq() (lastSeq uint64, err error) {
	kvStoreInfo, err := bucket.kvstore.Info()
	if err != nil {
		return uint64(0), err
	}
	return uint64(kvStoreInfo.LastSeqNum()), nil
}
