package forestbucket

import (
	"log"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestBucketURLToDir(t *testing.T) {

	var tests = []struct {
		url           string
		expectedDir   string
		shouldHaveErr bool
	}{
		{"forestdb:/foo/bar", "/foo/bar", false},
		{"forestdb:baz", "baz", false},
		{"/foo", "", true},
		{"", "", true},
	}

	for _, test := range tests {
		dir, err := bucketURLToDir(test.url)
		log.Printf("url: %v, dir: %v, err: %v", test.url, dir, err)
		hasErr := err != nil
		assert.Equals(t, test.shouldHaveErr, hasErr)
		if !hasErr {
			assert.Equals(t, dir, test.expectedDir)
		}
	}

}
