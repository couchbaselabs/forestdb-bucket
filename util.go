package forestbucket

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"

	"github.com/couchbaselabs/logg"
	"github.com/couchbaselabs/walrus"
	"github.com/nu7hatch/gouuid"
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
func AddTestDesignDoc(bucket walrus.Bucket) {

	// add design doc and map
	mapFunc := `function(doc){if (doc.key) emit(doc.key,doc.value)}`
	ddoc := walrus.DesignDoc{
		Views: walrus.ViewMap{
			"view1": walrus.ViewDef{
				Map: mapFunc,
			},
		},
	}

	err := bucket.PutDDoc("docname", ddoc)
	if err != nil {
		panic("Failed to put doc")
	}

}

// Add benchmark related code here so it can be re-used in an actual benchmark
// as well as in util/benchmark_webserver
func AddTestDocQueryView(bucket walrus.Bucket, i int) {

	key := AddTestDoc(bucket, i)
	QueryTestView(bucket, i, key)

}

func AddTestDoc(bucket walrus.Bucket, i int) string {

	// add doc to bucket
	docId := fmt.Sprintf("doc-%v", i)
	key := fmt.Sprintf("key-%v", i)
	value := fmt.Sprintf("val-%v", i)
	jsonStr := fmt.Sprintf(`{"key": "%v", "value": "%v"}`, key, value)
	err := setJSON(bucket, docId, jsonStr)
	if err != nil {
		panic("Failed to put doc")
	}
	return key

}

func QueryTestView(bucket walrus.Bucket, i int, key string) {

	// query view for doc
	options := map[string]interface{}{
		"key":   key,
		"stale": false,
	}
	result, err := bucket.View("docname", "view1", options)
	if err != nil {
		panic("Failed to query view")
	}
	log.Printf("result: %v", result)

}

func setJSON(bucket walrus.Bucket, docid string, jsonDoc string) error {
	var obj interface{}
	err := json.Unmarshal([]byte(jsonDoc), &obj)
	if err != nil {
		return err
	}
	return bucket.Set(docid, 0, obj)
}

func GetTestBucket() (bucket walrus.Bucket, tempDir string) {

	bucketUuid := NewUuid()
	tempDir = filepath.Join(os.TempDir(), bucketUuid)

	forestBucketUrl := fmt.Sprintf("forestdb:%v", tempDir)
	bucketName := fmt.Sprintf("testbucket-%v", bucketUuid)

	bucket, err := GetBucket(
		forestBucketUrl,
		DefaultPoolName,
		bucketName,
	)

	if err != nil {
		log.Panicf("Error creating bucket: %v", err)
	}

	return bucket, tempDir

}

func NewUuid() string {
	u4, err := uuid.NewV4()
	if err != nil {
		logg.LogPanic("Error generating uuid", err)
	}
	return fmt.Sprintf("%s", u4)
}
