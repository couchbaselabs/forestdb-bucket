package forestbucket

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/logg"
	"github.com/nu7hatch/gouuid"
)

func assertNoError(t *testing.T, err error, message string) {
	if err != nil {
		t.Fatalf("%s: %v", message, err)
	}
}

func assertTrue(t *testing.T, success bool, message string) {
	if !success {
		t.Fatalf("%s", message)
	}
}

func setJSON(bucket sgbucket.Bucket, docid string, jsonDoc string) error {
	var obj interface{}
	err := json.Unmarshal([]byte(jsonDoc), &obj)
	if err != nil {
		return err
	}
	return bucket.Set(docid, 0, obj)
}

func GetTestBucket() (bucket sgbucket.Bucket, tempDir string) {

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

func AddTestDocQueryView(bucket sgbucket.Bucket, i int) {

	key := AddTestDoc(bucket, i)
	QueryTestView(bucket, i, key)

}

func AddTestDoc(bucket sgbucket.Bucket, i int) string {

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
