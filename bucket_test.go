package forestbucket

import (
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestGetBucket(t *testing.T) {

	bucket, err := GetBucket(
		"forestdb:/foo/bar",
		"default",
		"testbucket",
	)
	assert.True(t, err == nil)
	assert.True(t, bucket != nil)

}
