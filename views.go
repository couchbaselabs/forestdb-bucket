package forestbucket

import (
	"encoding/json"

	"github.com/couchbaselabs/goforestdb"
	"github.com/couchbaselabs/walrus"
)

// A single view stored in a forestdb bucket.
type forestdbView struct {
	mapFunction         *walrus.JSMapFunction // The compiled map function
	reduceFunction      string                // The source of the reduce function (if any)
	index               walrus.ViewResult     // The latest complete result
	lastIndexedSequence uint64                // Bucket's lastSeq at the time the index was built
}

// Stores view functions for use by a forestdb bucket.
type forestdbDesignDoc map[string]*forestdbView

func (bucket *forestdbBucket) GetDDoc(docname string, into interface{}) error {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	rawDdoc, err := bucket.kvstoreDdocs.GetKV([]byte(docname))
	if err != nil {
		if err == forestdb.RESULT_KEY_NOT_FOUND {
			return walrus.MissingError{
				Key: docname,
			}
		}
		return err
	}
	// Have to roundtrip thru JSON to return it as arbitrary interface{}:
	raw, err := json.Marshal(rawDdoc)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, into)
}

func (bucket *forestdbBucket) PutDDoc(docname string, value interface{}) error {
	design, err := walrus.CheckDDoc(value)
	if err != nil {
		return err
	}

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	err = bucket._compileDesignDoc(docname, design)
	if err != nil {
		return err
	}

	raw, err := json.Marshal(design)
	if err != nil {
		return err
	}

	if err := bucket.kvstoreDdocs.SetKV([]byte(docname), raw); err != nil {
		return err
	}

	if err := bucket.db.Commit(forestdb.COMMIT_NORMAL); err != nil {
		return err
	}

	return nil

}

func (bucket *forestdbBucket) DeleteDDoc(docname string) error {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	if err := bucket.kvstoreDdocs.DeleteKV([]byte(docname)); err != nil {
		return err
	}

	delete(bucket.views, docname)
	return nil
}

func (bucket *forestdbBucket) _compileDesignDoc(docname string, design *walrus.DesignDoc) error {
	if design == nil {
		return nil
	}
	ddoc := forestdbDesignDoc{}
	for name, fns := range design.Views {
		jsserver := walrus.NewJSMapFunction(fns.Map)
		view := &forestdbView{
			mapFunction:    jsserver,
			reduceFunction: fns.Reduce,
		}
		ddoc[name] = view
	}
	bucket.views[docname] = ddoc
	return nil
}
