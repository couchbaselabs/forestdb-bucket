package forestbucket

import (
	"os"
	"testing"

	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/walrus"
)

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

func BenchmarkView(b *testing.B) {

	bucket, tempDir := GetTestBucket()
	defer func() {
		CloseBucket(bucket)
		os.RemoveAll(tempDir)
	}()

	AddTestDesignDoc(bucket)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		AddTestDocQueryView(bucket, i)

	}

}
