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

	log.Printf("forestdbBucket View() called with: (%q, %q)", docName, viewName)

	stale := true
	if params != nil {
		if staleParam, found := params["stale"].(bool); found {
			stale = staleParam
		}
	}

	// Look up the view and its index:
	var result walrus.ViewResult
	view, resultMaybe, err := bucket.findView(docName, viewName, stale)
	if err != nil {
		return result, err
	}
	if view == nil {
		log.Printf("View not found")
		return result, walrus.MissingError{Key: docName + "/" + viewName}
	} else if resultMaybe != nil {
		log.Printf("Got resultMaybe: %+v", *resultMaybe)
		result = *resultMaybe
	} else {
		log.Printf("Going to update view")
		result = bucket.updateView(view, 0)
	}

	log.Printf("Going to process view result: %+v", result)
	return walrus.ProcessViewResult(result, params, bucket, view.reduceFunction)

}

// Looks up a lolrusView, and its current index if it's up-to-date enough.
// TODO: consolidate with walrus codebase to fix code duplication
func (bucket *forestdbBucket) findView(docName, viewName string, staleOK bool) (view *forestdbView, result *walrus.ViewResult, err error) {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	if ddoc, exists := bucket.views[docName]; exists {
		view = ddoc[viewName]
		if view != nil {

			lastSeq, err := bucket.LastSeq()
			if err != nil {
				return view, result, err
			}

			log.Printf("Check if view up to date.  view.lastIndexedSequence: %v, bucket.LastSeq: %v", view.lastIndexedSequence, lastSeq)

			upToDate := view.lastIndexedSequence == lastSeq

			if !upToDate && view.lastIndexedSequence > 0 && staleOK {
				log.Printf("View not up to date, calling updateView()")
				go bucket.updateView(view, lastSeq)
				upToDate = true
			} else {
				log.Printf("View already up to date")
			}
			if upToDate {
				curResult := view.index // copy the struct
				result = &curResult
			}
		}
	}
	return view, result, nil
}

// Updates the view index if necessary, and returns it.
// TODO: consolidate with walrus codebase to fix code duplication
func (bucket *forestdbBucket) updateView(view *forestdbView, toSequence uint64) walrus.ViewResult {

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	result := walrus.ViewResult{}

	lastSeq, err := bucket.LastSeq()
	if err != nil {
		log.Printf("Error updating view, LastSeq() returned err: %v", err)
		return result
	}

	if toSequence == 0 {
		toSequence = lastSeq
	}
	if view.lastIndexedSequence >= toSequence {
		return view.index
	}
	log.Printf("\t... updating index to seq %d (from %d)", toSequence, view.lastIndexedSequence)

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
		log.Printf("Mapper got input: %v", rawInput)
		input := rawInput.([2]string)
		docid := input[0]
		raw := input[1]
		rows, err := mapFunction.CallFunction(string(raw), docid)
		log.Printf("Mapper produced rows: %v", rows)
		if err != nil {
			log.Printf("Error running map function: %s", err)
			output <- walrus.ViewError{
				From:   docid,
				Reason: err.Error(),
			}
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

	// TODO: this should be starting from view.lastIndexedSequence
	startSequence := 0

	// create an iterator
	options := forestdb.ITR_NONE
	endSeq := forestdb.SeqNum(0) // is this how to specify no end sequence?
	iterator, err := bucket.kvstore.IteratorSequenceInit(
		forestdb.SeqNum(startSequence),
		endSeq,
		options,
	)
	if err != nil {
		// TODO: this should return an error instead
		log.Panicf("Could not create forestdb iterator on: %v", bucket)
	}

	for {
		doc, err := iterator.Get()
		if err != nil {
			log.Printf("Error getting doc from iterator: %v, break out of loop", err)
			break
		} else {
			log.Printf("Got a doc from iterator.  key: %v", string(doc.Key()))
		}

		log.Printf("Check if seqnum > lastIndexed.  doc.seqnum: %v, lastIndexed: %v", doc.SeqNum(), view.lastIndexedSequence)

		if uint64(doc.SeqNum()) > view.lastIndexedSequence {
			log.Printf("Yes, seqnum > lastIndexed")

			raw := doc.Body()
			log.Printf("raw doc body: %v", string(raw))
			docid := string(doc.Key())
			if raw != nil {
				if !isJSON(raw) {
					log.Printf("its not json, set to empty dict")
					raw = []byte(`{}`) // Ignore contents of non-JSON (raw) docs
				}
				log.Printf("send to map input channel")
				mapInput <- [2]string{docid, string(raw)}
				updatedKeys[docid] = struct{}{}
			}

		} else {
			log.Printf("No, seqnum not greater than lastIndexed")
		}

		if err := iterator.Next(); err != nil {
			// we're done.
			log.Printf("iterator.next() returned an error: %v, break out of loop", err)
			break
		}

	}
	close(mapInput)

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

	view.lastIndexedSequence = lastSeq
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
