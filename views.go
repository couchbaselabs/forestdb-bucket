package forestbucket

import (
	"encoding/json"
	"log"
	"sort"
	"sync"

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

func (bucket *forestdbBucket) View(docName, viewName string, params map[string]interface{}) (walrus.ViewResult, error) {

	log.Printf("View(%q, %q) ...", docName, viewName)

	stale := true
	if params != nil {
		if staleParam, found := params["stale"].(bool); found {
			stale = staleParam
		}
	}

	// Look up the view and its index:
	var result walrus.ViewResult
	view, resultMaybe := bucket.findView(docName, viewName, stale)
	if view == nil {
		return result, walrus.MissingError{Key: docName + "/" + viewName}
	} else if resultMaybe != nil {
		result = *resultMaybe
	} else {
		result = bucket.updateView(view, 0)
	}

	return walrus.ProcessViewResult(result, params, bucket, view.reduceFunction)

}

// Looks up a lolrusView, and its current index if it's up-to-date enough.
// TODO: consolidate with walrus codebase to fix code duplication
func (bucket *forestdbBucket) findView(docName, viewName string, staleOK bool) (view *forestdbView, result *walrus.ViewResult) {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	if ddoc, exists := bucket.views[docName]; exists {
		view = ddoc[viewName]
		if view != nil {
			upToDate := view.lastIndexedSequence == bucket.LastSeq
			if !upToDate && view.lastIndexedSequence > 0 && staleOK {
				go bucket.updateView(view, bucket.LastSeq)
				upToDate = true
			}
			if upToDate {
				curResult := view.index // copy the struct
				result = &curResult
			}
		}
	}
	return
}

// Updates the view index if necessary, and returns it.
// TODO: consolidate with walrus codebase to fix code duplication
func (bucket *forestdbBucket) updateView(view *forestdbView, toSequence uint64) walrus.ViewResult {

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	if toSequence == 0 {
		toSequence = bucket.LastSeq
	}
	if view.lastIndexedSequence >= toSequence {
		return view.index
	}
	log.Printf("\t... updating index to seq %d (from %d)", toSequence, view.lastIndexedSequence)

	var result walrus.ViewResult
	result.Rows = make([]*walrus.ViewRow, 0)
	result.Errors = make([]walrus.ViewError, 0)

	updatedKeysSize := toSequence - view.lastIndexedSequence
	if updatedKeysSize > 1000 {
		updatedKeysSize = 1000
	}
	updatedKeys := make(map[string]struct{}, updatedKeysSize)

	// Build a parallel task to map docs:
	mapFunction := view.mapFunction
	mapper := func(rawInput interface{}, output chan<- interface{}) {
		input := rawInput.([2]string)
		docid := input[0]
		raw := input[1]
		rows, err := mapFunction.CallFunction(string(raw), docid)
		if err != nil {
			log.Printf("Error running map function: %s", err)
			output <- walrus.ViewError{docid, err.Error()}
		} else {
			output <- rows
		}
	}
	mapInput := make(chan interface{})
	mapOutput := walrus.Parallelize(mapper, 0, mapInput)

	// Start another task to read the map output and store it into result.Rows/Errors:
	var waiter sync.WaitGroup
	waiter.Add(1)
	go func() {
		defer waiter.Done()
		for item := range mapOutput {
			switch item := item.(type) {
			case walrus.ViewError:
				result.Errors = append(result.Errors, item)
			case []*walrus.ViewRow:
				result.Rows = append(result.Rows, item...)
			}
		}
	}()

	// TODO: forestdb iterator

	// Now shovel all the changed document bodies into the mapper:
	/*
		for docid, doc := range bucket.Docs {
			if doc.Sequence > view.lastIndexedSequence {
				raw := doc.Raw
				if raw != nil {
					if !doc.IsJSON {
						raw = []byte(`{}`) // Ignore contents of non-JSON (raw) docs
					}
					mapInput <- [2]string{docid, string(raw)}
					updatedKeys[docid] = struct{}{}
				}
			}
		}
		close(mapInput)
	*/

	// Wait for the result processing to finish:
	waiter.Wait()

	// Copy existing view rows emitted by unchanged docs:
	for _, row := range view.index.Rows {
		if _, found := updatedKeys[row.ID]; !found {
			result.Rows = append(result.Rows, row)
		}
	}
	for _, err := range view.index.Errors {
		if _, found := updatedKeys[err.From]; !found {
			result.Errors = append(result.Errors, err)
		}
	}

	sort.Sort(&result)
	result.Collator.Clear() // don't keep collation state around

	view.lastIndexedSequence = bucket.LastSeq
	view.index = result
	return view.index
}

func (bucket *forestdbBucket) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	return nil
}

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

	return json.Unmarshal(rawDdoc, into)
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
