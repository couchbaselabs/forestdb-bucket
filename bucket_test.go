package forestbucket

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/goforestdb"
	"github.com/couchbaselabs/walrus"
)

// If we try to Get() a missing key, asser that a walrus.MissingError is returned
func TestGetMissingKey(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	var value interface{}
	err := bucket.Get("missingkey", &value)
	assert.True(t, err != nil)
	log.Printf("err: %v type: %T", err, err)
	_, ok := err.(walrus.MissingError)
	assert.True(t, ok)

}

func TestUpdate(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	testDocBytes := []byte(`{"foo":"bar"}`)

	// UpdateFunc func(current []byte) (updated []byte, err error)
	updateFunc := func(current []byte) (updated []byte, err error) {
		log.Printf("current: %v", current)
		if current == nil {
			log.Printf("current == nil")
		}
		if len(current) == 0 {
			log.Printf("len(current) == 0")
		}
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
	assert.Equals(t, string(fetchedDoc), string(testDocBytes))

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

	// Try a ViewCustom query
	customResult := map[string]interface{}{}
	err = bucket.ViewCustom("docname", "view1", options, &customResult)
	assert.True(t, err == nil)
	totalRowsRaw, ok := customResult["total_rows"]
	assert.True(t, ok)
	totalRows, ok := totalRowsRaw.(float64) // TODO: why isn't this an int?
	assert.True(t, ok)
	assert.Equals(t, totalRows, float64(5.0))

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
	var value2 interface{}
	var value3 interface{}

	err := bucket.Get("key", &value)
	assert.True(t, err != nil)

	added, err := bucket.Add("key", 0, "value")
	assertNoError(t, err, "Add")
	assert.True(t, added)
	assertNoError(t, bucket.Get("key", &value), "Get")
	assert.Equals(t, value, "value")

	assertNoError(t, bucket.Delete("key"), "Delete")
	err = bucket.Get("key", &value2)
	_, ok := err.(walrus.MissingError) // make sure right type of error
	assert.True(t, ok)
	log.Printf("err: %v", err)
	assert.True(t, err != nil)

	added, err = bucket.Add("key", 0, "value")
	assertNoError(t, err, "Add")
	assert.True(t, added)

	err = bucket.Get("key", &value3)
	assert.True(t, err == nil)
	log.Printf("value3: %v", value3)
	assert.Equals(t, value3, "value")

}

// Verify that if you delete a doc from bucket, and then call
// Update(), it will call Update() with a current value of nil
func TestDeleteThenUpdate(t *testing.T) {

	var value interface{}

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	// Add
	added, err := bucket.Add("key", 0, "value")
	assertNoError(t, err, "Add")
	assert.True(t, added)
	assertNoError(t, bucket.Get("key", &value), "Get")
	assert.Equals(t, value, "value")

	// Delete
	var valuePostDelete interface{}
	assertNoError(t, bucket.Delete("key"), "Delete")
	err = bucket.Get("key", &valuePostDelete)
	log.Printf("value post delete: %v", valuePostDelete)
	assert.True(t, err != nil)
	assert.True(t, valuePostDelete == nil)

	// Update
	updateFunc := func(current []byte) (updated []byte, err error) {
		// since we deleted the value, we expect current to be nil
		log.Printf("UpdateFunc called")
		if current != nil {
			log.Printf("current != nil")
			return nil, fmt.Errorf("Expected current to be nil")
		} else {
			log.Printf("current is nil")
		}
		return []byte("some_value"), nil
	}
	err = bucket.Update("key", 0, updateFunc)

	assert.True(t, err == nil)

}

// Unexpected behavior with Goforestdb -- set a doc to nil value, but
// when fetching it, it returns an empty slice.
// See https://github.com/couchbase/goforestdb/issues/15
func DisabledTestSetDocBodyToNil(t *testing.T) {

	rawBucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(rawBucket)

	bucket := rawBucket.(*forestdbBucket)

	// set "foo" key to value "bar"
	err := bucket.kvstore.SetKV([]byte("foo"), []byte("bar"))
	assert.True(t, err == nil)

	err = bucket.db.Commit(forestdb.COMMIT_NORMAL)
	assert.True(t, err == nil)

	// now set "foo" key to nil
	doc, err := forestdb.NewDoc([]byte("foo"), nil, nil)
	assert.True(t, err == nil)

	defer doc.Close()
	err = bucket.kvstore.Set(doc)
	assert.True(t, err == nil)

	err = bucket.db.Commit(forestdb.COMMIT_NORMAL)
	assert.True(t, err == nil)

	// get the value of "foo" key
	docBody, err := bucket.kvstore.GetKV([]byte("foo"))
	assert.True(t, err == nil)
	assert.True(t, docBody == nil) // fails, because it's an empty slice

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

// emulates what happens in the Sync Gateway Flush() scenario
// where the bucket is closed twice.
func TestCloseTwice(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)

	if bucket, ok := bucket.(walrus.DeleteableBucket); ok {
		bucket.Close()
		err := bucket.CloseAndDelete()
		log.Printf("CloseAndDelete() returned: %v", err)
		assert.True(t, err == nil)
	} else {
		log.Fatalf("Bucket does not support Deletable interface")
	}

}

// While running gateload, seeing an error where:
// - Does a PUT to create a user
// - Try to update the user
// - Actual: update fails since cannot find value for key (unexpected)
func TestWriteUpdateConsistency(t *testing.T) {

	// failure scenario:
	// set docId=foo to value=bar
	// goroutine1 calls WriteUpdate on docId=foo
	// goroutine1 calls Get(), doc is at seq=1
	// goroutine2 calls SetRaw(foo, bar2)
	// goroutine1 calls callback with currentVal=bar (stale!!)
	//   Error: goroutine1 ignores the value set by goroutine2,
	//          and is doing update on a stale value

	// reproduce:
	// - two goroutines which have update functions that increment
	// value by 1 and emit new value to a channel
	// - listener which makes sure that every update is monontonically increaseing, ie:
	//   if there two updates from 1->2, that means that there was a stale update

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	key := "key"
	numIterations := 50

	updateChan := make(chan int)

	// set the value to 0 to start with
	added, err := bucket.AddRaw(key, 0, []byte("0"))
	if !added || err != nil {
		log.Panicf("failed to add value")
	}

	updateLoopFunc := func() {
		for i := 0; i < numIterations; i++ {

			incrementedNum := -1
			currentNum := -1
			updateFunc := func(current []byte) (updated []byte, err error) {
				if current == nil {
					log.Panicf("should not be nil, we set this value earlier")
				}
				if len(current) == 0 {
					log.Panicf("should not be empty, we set this value earlier")
				}
				currentStr := string(current)
				currentNum, err = strconv.Atoi(currentStr)
				if err != nil {
					log.Panicf("unexpected error doing Atoi: %v", err)
				}
				incrementedNum = currentNum + 1
				incrementedNumStr := fmt.Sprintf("%v", incrementedNum)
				return []byte(incrementedNumStr), nil
			}

			err := bucket.Update(key, 0, updateFunc)
			if err != nil {
				log.Panicf("unexpected error updating bucket: %v", err)
			}

			updateChan <- incrementedNum
		}

	}

	go updateLoopFunc()
	go updateLoopFunc()

	// the expectation is that all numbers coming across the updateChan
	// must be monotonically increasing.  If dupes appear, that means the
	// goroutines are stepping on eachother and doing stale writes
	dupeMap := map[int]int{}
	i := 0
	for update := range updateChan {

		// introduce an artificial delay to help reproduce race conditions
		<-time.After(1 * time.Millisecond)

		// make sure we haven't seen this number yet
		_, ok := dupeMap[update]
		if ok {
			log.Panicf("already seen: %v, numbers should be monotonically increasing with no dupes", update)
		}
		dupeMap[update] = update
		i += 1
		// log.Printf("i: %v", i)
		if i >= (numIterations * 2) {
			// we've seen all the numbers, we're done
			break
		}
	}

}

// There is currently a bug where:
// Goroutine1 calls SetRaw to set doc with key=foo to value=bar
// Goroutine2 calls WriteUpdate
//   - Calls kvstore.Get(foo), *without* holding a lock on the kvstore
//   - Gets "key not found"
// NOTE: run this test with GOMAXPROCS set to >= 2
// NOTE: this test only fails sporadically, approx ~50% of the time
func TestWriteUpdateInconsistentRead(t *testing.T) {

	// this is a relatively long running test, so skip it if -short flag passed
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	// this was chosen to be high enough to make it fail as often as possible
	// while keeping the test runtime as low as possible.
	numKeys := 5000

	wg := sync.WaitGroup{}
	wg.Add(numKeys)

	for i := 0; i < numKeys; i++ {

		go func() {
			key := NewUuid()
			data := []byte(key)
			if err := bucket.SetRaw(key, 0, data); err != nil {
				log.Panicf("Unable to set key: %v", key)
			}

			go func() {
				updateFunc := func(current []byte) (updated []byte, err error) {
					if current == nil {
						log.Panicf("should not be nil, we set this value earlier")
					}
					if len(current) == 0 {
						log.Panicf("should not be empty, we set this value earlier")
					}
					return current, nil
				}
				err := bucket.Update(key, 0, updateFunc)
				if err != nil {
					log.Panicf("Got error calling update: %v", err)
				}
				wg.Done()

			}()
		}()

	}

	wg.Wait()

}
