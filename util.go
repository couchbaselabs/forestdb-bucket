package forestbucket

import (
	"encoding/json"
	"fmt"
	"net/url"
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
