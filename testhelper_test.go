package forestbucket

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

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
