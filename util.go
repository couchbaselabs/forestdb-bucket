package forestbucket

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/couchbase/sg-bucket"
)

// Interprets a bucket urlStr as a directory, or returns an error if it's not.
func bucketURLToDir(urlStr string) (dir string, err error) {
	urlobj, err := url.Parse(urlStr)
	if err != nil {
		return "", err
	}
	if urlobj.Scheme != "forestdb" {
		return "", fmt.Errorf("Invalid scheme: %v", urlStr)
	}

	dir = urlobj.Path
	if dir == "" {
		dir = urlobj.Opaque
	}

	return dir, nil

}

func copySlice(slice []byte) []byte {
	if slice == nil {
		return nil
	}
	copied := make([]byte, len(slice))
	copy(copied, slice)
	return copied
}

func isJSON(raw []byte) bool {
	var js interface{}
	return json.Unmarshal(raw, &js) == nil

}

// Add benchmark related code here so it can be re-used in an actual benchmark
// as well as in util/benchmark_webserver
func AddTestDesignDoc(bucket sgbucket.Bucket) {

	// add design doc and map
	mapFunc := `function(doc){if (doc.key) emit(doc.key,doc.value)}`
	ddoc := sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"view1": sgbucket.ViewDef{
				Map: mapFunc,
			},
		},
	}

	err := bucket.PutDDoc("docname", ddoc)
	if err != nil {
		panic("Failed to put doc")
	}

}

func QueryTestView(bucket sgbucket.Bucket, i int, key string) {

	// query view for doc
	options := map[string]interface{}{
		"key":   key,
		"stale": false,
	}
	_, err := bucket.View("docname", "view1", options)
	if err != nil {
		panic("Failed to query view")
	}

}
