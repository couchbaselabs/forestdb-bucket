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

	log.Printf("forestdb NewBucket() with path: %v, pool: %v, name: %v", bucketRootPath, poolName, bucketName)

	bucket := &forestdbBucket{
		name:           bucketName,
		bucketRootPath: bucketRootPath,
		poolName:       poolName,
	}

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

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

// TODO: remove code duplication with GetRaw()
func (bucket *forestdbBucket) Get(key string, returnVal interface{}) error {

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	// Lookup the document
	doc, err := forestdb.NewDoc([]byte(key), nil, nil)
	if err != nil {
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

	// workaround hack: in order to get Sync Gateway's TestLocalDocs() to
	// work, as well as the new version of TestDeleteThenAdd, treat an
	// empty value as missing.  TODO: I'm still confused as to why
	// Delete() shouldn't just call ForestDB delete, rather than trying
	// to set to a nil value.  (See https://github.com/couchbase/goforestdb/issues/15)
	if doc.Body() == nil || len(doc.Body()) == 0 {
		return walrus.MissingError{
			Key: key,
		}
	}

	if !doc.Deleted() {
		return json.Unmarshal(doc.Body(), returnVal)
	}
	return nil
}

// TODO: remove code duplication with Get()
func (bucket *forestdbBucket) GetRaw(key string) ([]byte, error) {

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	// Lookup the document
	doc, err := forestdb.NewDoc([]byte(key), nil, nil)
	if err != nil {
		return nil, err
	}
	defer doc.Close()

	err = bucket.kvstore.Get(doc)
	if err != nil {
		if err == forestdb.RESULT_KEY_NOT_FOUND {
			return nil, walrus.MissingError{
				Key: key,
			}
		}
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
	if err := bucket.db.Commit(forestdb.COMMIT_NORMAL); err != nil {
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

	for {
		doc, err := forestdb.NewDoc([]byte(key), nil, nil)
		if err != nil {
			log.Printf("WriteUpdate couldn't create new doc: %v", err)
			return err
		}
		defer doc.Close()

		// needed to pass TestWriteUpdateInconsistentRead
		// TODO: is there a cleaner way?
		bucket.lock.Lock()
		err = bucket.kvstore.Get(doc)
		bucket.lock.Unlock()

		if err != nil && err != forestdb.RESULT_KEY_NOT_FOUND {
			// if it's an unexpected error, return it
			log.Printf("WriteUpdate got unexpected err calling Get(): %v", err)
			return err
		}

		docBody := doc.Body()

		// workaround hack: in the Delete() method, we set the doc body to nil.
		// however, when we fetch it, it comes back as an empty slice.  in order
		// to workaraound that behavior and get the TestDeleteThenUpdate() test to
		// pass, if docBody is empty, change it to nil
		if len(docBody) == 0 {
			docBody = nil
		}

		// TODO: is copySlice really needed here?
		docBodyCopy := copySlice(docBody)

		newDocBody, _, err := callback(docBodyCopy)
		if err != nil {
			return err
		}

		if docBody != nil && newDocBody == nil {
			// if the original doc body was not nil, but the callback returns a nil
			// newDocBody, then it appears that the callback is trying to delete
			// the doc.  (this is needed so that TestDeleteViaUpdate() passes)
			if err := bucket.Delete(key); err != nil {
				return err
			}
			return nil

		} else {
			// update doc with new body
			if err := doc.Update(doc.Meta(), newDocBody); err != nil {
				return err
			}

			seq, err := bucket.updateDoc(key, doc)
			if err != nil {
				return err
			}

			if seq > 0 {
				break
			}

		}

	}

	return nil

}

// Replaces a forestdb doc as long as its sequence number hasn't changed yet. (Used by Update)
func (bucket *forestdbBucket) updateDoc(key string, doc *forestdb.Doc) (seq uint64, err error) {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	curSequence := uint64(0)
	curDoc, err := forestdb.NewDoc([]byte(key), nil, nil)
	if err != nil {
		return 0, err
	}
	defer curDoc.Close()

	err = bucket.kvstore.Get(curDoc)
	if err != nil && err != forestdb.RESULT_KEY_NOT_FOUND {
		return 0, err
	}
	if err != forestdb.RESULT_KEY_NOT_FOUND {
		curSequence = uint64(curDoc.SeqNum())
		if curSequence != uint64(doc.SeqNum()) {
			return 0, nil
		}

	}

	// either doc does not already exist, or it's not stale
	// (eg, it's still at the sequence number our updated doc is based on)
	// so we can write the new doc.
	if err := bucket.kvstore.Set(doc); err != nil {
		return 0, err
	}
	if err := bucket.db.Commit(forestdb.COMMIT_NORMAL); err != nil {
		return 0, err
	}

	bucket._postTapMutationEvent(key, doc.Body(), uint64(doc.SeqNum()))
	return uint64(doc.SeqNum()), nil

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

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	if bucket.kvstore == nil || bucket.db == nil {
		// already closed, ignore this call
		return
	}

	bucket.kvstore.Close()
	bucket.db.Close()

	// set to nil so that we can't accidentally use the underlying
	// kvstore again for this bucket instance
	bucket.kvstore = nil
	bucket.kvstoreDdocs = nil
	bucket.db = nil
	bucket.views = nil

	// remove from bucket map so that if someone tries to get
	// this bucket, they'll get a new instance
	key := buckets.key(
		bucket.bucketRootPath,
		bucket.poolName,
		bucket.name,
	)
	buckets.delete(key)

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

func (bucket *forestdbBucket) CloseAndDelete() error {
	path := bucket.bucketDbFilePath()
	bucket.Close()
	if path == "" {
		return nil
	}
	return os.Remove(path)
}

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
