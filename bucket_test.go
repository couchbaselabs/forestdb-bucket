package forestbucket

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/goforestdb"
	"github.com/couchbaselabs/walrus"
)

func TestUpdate(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	testDocBytes := []byte(`{"foo":"bar"}`)

	// UpdateFunc func(current []byte) (updated []byte, err error)
	updateFunc := func(current []byte) (updated []byte, err error) {
		return testDocBytes, nil
	}
	err := bucket.Update("key", 0, updateFunc)
	log.Printf("update err: %v", err)
	assert.True(t, err == nil)

	// make sure we can get the doc by key and it has expected val
	var value interface{}
	err = bucket.Get("key", &value)
	assert.True(t, err == nil)

	fetchedDoc, _ := json.Marshal(value)
	log.Printf("fetched: %v", string(fetchedDoc))

	// valueMap := value.(map[string]string)
	// assert.Equals(t, valueMap["foo"], "bar")

}

// Create a simple view and run it on some documents
func TestView(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	mapFunc := `function(doc){if (doc.key) emit(doc.key,doc.value)}`
	ddoc := walrus.DesignDoc{
		Views: walrus.ViewMap{
			"view1": walrus.ViewDef{
				Map: mapFunc,
			},
		},
	}

	err := bucket.PutDDoc("docname", ddoc)
	assertNoError(t, err, "PutDDoc failed")

	var echo walrus.DesignDoc
	err = bucket.GetDDoc("docname", &echo)
	assert.DeepEquals(t, echo, ddoc)

	setJSON(bucket, "doc1", `{"key": "k1", "value": "v1"}`)
	setJSON(bucket, "doc2", `{"key": "k2", "value": "v2"}`)
	setJSON(bucket, "doc3", `{"key": 17, "value": ["v3"]}`)
	setJSON(bucket, "doc4", `{"key": [17, false], "value": null}`)
	setJSON(bucket, "doc5", `{"key": [17, true], "value": null}`)

	// raw docs and counters should not be indexed by views
	bucket.AddRaw("rawdoc", 0, []byte("this is raw data"))
	bucket.Incr("counter", 1, 0, 0)

	options := map[string]interface{}{"stale": false}
	result, err := bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equals(t, result.TotalRows, 5)
	assert.DeepEquals(t, result.Rows[0], &walrus.ViewRow{ID: "doc3", Key: 17.0, Value: []interface{}{"v3"}})
	assert.DeepEquals(t, result.Rows[1], &walrus.ViewRow{ID: "doc1", Key: "k1", Value: "v1"})
	assert.DeepEquals(t, result.Rows[2], &walrus.ViewRow{ID: "doc2", Key: "k2", Value: "v2"})
	assert.DeepEquals(t, result.Rows[3], &walrus.ViewRow{ID: "doc4", Key: []interface{}{17.0, false}})
	assert.DeepEquals(t, result.Rows[4], &walrus.ViewRow{ID: "doc5", Key: []interface{}{17.0, true}})

	// Try a startkey:
	options["startkey"] = "k2"
	options["include_docs"] = true
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equals(t, result.TotalRows, 3)
	var expectedDoc interface{} = map[string]interface{}{"key": "k2", "value": "v2"}
	assert.DeepEquals(t, result.Rows[0], &walrus.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc})

	// Try an endkey:
	options["endkey"] = "k2"
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equals(t, result.TotalRows, 1)
	assert.DeepEquals(t, result.Rows[0], &walrus.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc})

	// Try an endkey out of range:
	options["endkey"] = "k999"
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equals(t, result.TotalRows, 1)
	assert.DeepEquals(t, result.Rows[0], &walrus.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc})

	// Try without inclusive_end:
	options["endkey"] = "k2"
	options["inclusive_end"] = false
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equals(t, result.TotalRows, 0)

	// Try a single key:
	options = map[string]interface{}{"stale": false, "key": "k2", "include_docs": true}
	result, err = bucket.View("docname", "view1", options)
	assertNoError(t, err, "View call failed")
	assert.Equals(t, result.TotalRows, 1)
	assert.DeepEquals(t, result.Rows[0], &walrus.ViewRow{ID: "doc2", Key: "k2", Value: "v2",
		Doc: &expectedDoc})

	// Delete the design doc:
	assertNoError(t, bucket.DeleteDDoc("docname"), "DeleteDDoc")
	assert.DeepEquals(t, bucket.GetDDoc("docname", &echo), walrus.MissingError{Key: "docname"})
}

func TestCheckDDoc(t *testing.T) {
	ddoc := walrus.DesignDoc{Views: walrus.ViewMap{"view1": walrus.ViewDef{Map: `function(doc){if (doc.key) emit(doc.key,doc.value)}`}}}
	_, err := walrus.CheckDDoc(&ddoc)
	assertNoError(t, err, "CheckDDoc should have worked")

	ddoc = walrus.DesignDoc{Language: "go"}
	_, err = walrus.CheckDDoc(&ddoc)
	assertTrue(t, err != nil, "CheckDDoc should have rejected non-JS")
}

func TestDurableAdd(t *testing.T) {

	key := "key"
	val := map[string]string{
		"foo": "bar",
	}

	// get a test bucket
	bucket, tempDir := GetTestBucket()

	// clean up dir when we're done
	defer os.RemoveAll(tempDir)

	// get the file path
	fdbBucket := bucket.(*forestdbBucket)
	dbFile := fdbBucket.bucketDbFilePath()

	// add a key
	added, err := bucket.Add(key, 0, val)
	assertNoError(t, err, "Add")
	assert.True(t, added)

	// close the bucket
	CloseBucket(bucket)

	// open forestdb database file + kvstore
	db, err := forestdb.Open(dbFile, nil)
	assert.True(t, err == nil)
	kvstore, err := db.OpenKVStoreDefault(nil)
	assert.True(t, err == nil)

	// make sure we can get the doc by key and it has expected val
	doc, err := forestdb.NewDoc([]byte(key), nil, nil)
	assert.True(t, err == nil)
	defer doc.Close()
	err = kvstore.Get(doc)
	assert.True(t, err == nil)
	assert.True(t, len(doc.Body()) > 0)
	docJson := make(map[string]string)
	err = json.Unmarshal(doc.Body(), &docJson)
	assert.True(t, err == nil)
	assert.Equals(t, docJson["foo"], "bar")

}

func TestDeleteThenAdd(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	var value interface{}
	err := bucket.Get("key", &value)
	assert.True(t, err != nil)

	added, err := bucket.Add("key", 0, "value")
	assertNoError(t, err, "Add")
	assert.True(t, added)
	assertNoError(t, bucket.Get("key", &value), "Get")
	assert.Equals(t, value, "value")

	assertNoError(t, bucket.Delete("key"), "Delete")
	err = bucket.Get("key", &value)
	assert.True(t, err != nil)
	added, err = bucket.Add("key", 0, "value")
	assertNoError(t, err, "Add")
	assert.True(t, added)

}

func TestIncr(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	count, err := bucket.Incr("count1", 1, 100, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, count, uint64(100))

	count, err = bucket.Incr("count1", 0, 0, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, count, uint64(100))

	count, err = bucket.Incr("count1", 10, 100, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, count, uint64(110))

	count, err = bucket.Incr("count1", 0, 0, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, count, uint64(110))

	count, err = bucket.Incr("count2", 0, 0, -1)
	assertTrue(t, err != nil, "Expected error from Incr")

}

// Spawns 1000 goroutines that 'simultaneously' use Incr to increment the same counter by 1.
func TestIncrAtomic(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	var waiters sync.WaitGroup
	numIncrements := 1000
	waiters.Add(numIncrements)
	for i := uint64(1); i <= uint64(numIncrements); i++ {
		numToAdd := i // lock down the value for the goroutine
		go func() {
			_, err := bucket.Incr("key", numToAdd, numToAdd, 0)
			assertNoError(t, err, "Incr")
			waiters.Add(-1)
		}()
	}
	waiters.Wait()
	value, err := bucket.Incr("key", 0, 0, 0)
	assertNoError(t, err, "Incr")
	assert.Equals(t, int(value), numIncrements*(numIncrements+1)/2)
}

func TestAppend(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	err := bucket.Append("key", []byte(" World"))
	assert.DeepEquals(t, err, walrus.MissingError{
		Key: "key",
	})

	log.Printf("call SetRaw")
	err = bucket.SetRaw("key", 0, []byte("Hello"))
	assertNoError(t, err, "SetRaw")
	err = bucket.Append("key", []byte(" World"))
	assertNoError(t, err, "Append")
	value, err := bucket.GetRaw("key")
	assertNoError(t, err, "GetRaw")
	assert.DeepEquals(t, value, []byte("Hello World"))
}

func TestGetBucket(t *testing.T) {

	tempDir := os.TempDir()
	defer os.RemoveAll(tempDir)

	forestBucketUrl := fmt.Sprintf("forestdb:%v", tempDir)
	bucketName := "testbucket"

	// get a bucket
	bucket, err := GetBucket(
		forestBucketUrl,
		DefaultPoolName,
		bucketName,
	)
	defer CloseBucket(bucket)

	assert.True(t, err == nil)
	assert.True(t, bucket != nil)
	assert.Equals(t, bucket.GetName(), bucketName)

	// make sure it created the path for the bucket
	bucketPath := filepath.Join(tempDir, bucketName)
	_, err = os.Stat(bucketPath)
	assert.False(t, os.IsNotExist(err))

	// get the same bucket again, make sure it's the same
	bucketCopy, err := GetBucket(
		forestBucketUrl,
		DefaultPoolName,
		bucketName,
	)
	assert.True(t, err == nil)
	assert.Equals(t, bucket, bucketCopy)

}

func TestGetInvalidBucket(t *testing.T) {

	// get a bucket with an invalid url, assert error
	_, err := GetBucket(
		":invalid_url:",
		DefaultPoolName,
		"testbucket",
	)
	assert.True(t, err != nil)

}

func TestGetBucketNoPoolName(t *testing.T) {

	tempDir := os.TempDir()
	defer os.RemoveAll(tempDir)

	forestBucketUrl := fmt.Sprintf("forestdb:%v", tempDir)

	// Get a bucket with no pool name
	bucket, err := GetBucket(
		forestBucketUrl,
		"",
		"testbucket",
	)
	assert.True(t, err == nil)
	assert.True(t, bucket != nil)

}
