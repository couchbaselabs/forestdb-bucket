package forestbucket

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/couchbaselabs/logg"
	"github.com/couchbaselabs/walrus"
	"github.com/nu7hatch/gouuid"
)

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

func setJSON(bucket walrus.Bucket, docid string, jsonDoc string) error {
	var obj interface{}
	err := json.Unmarshal([]byte(jsonDoc), &obj)
	if err != nil {
		return err
	}
	return bucket.Set(docid, 0, obj)
}
