package forestbucket

import (
	"fmt"
	"net/url"
	"strings"
)

// Interprets a bucket urlStr as a directory, or returns an error if it's not.
func bucketURLToDir(urlStr string) (dir string, err error) {
	urlobj, err := url.Parse(urlStr)
	if err != nil {
		return "", err
	}
	if urlobj == nil {
		return "", fmt.Errorf("unable to parse url")
	}
	if urlobj.Scheme != "forestdb" {
		return "", fmt.Errorf("Invalid scheme: %v", urlStr)
	}

	dir = urlobj.Path
	if dir == "" {
		dir = urlobj.Opaque
	} else if strings.HasPrefix(dir, "//") {
		dir = dir[2:]
	}
	if dir == "/" {
		dir = ""
	}

	return dir, nil

}
