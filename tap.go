package forestbucket

import (
	"github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/go-safe-dstruct/queue"
	"github.com/couchbaselabs/goforestdb"
)

type tapFeedImpl struct {
	bucket  *forestdbBucket
	channel chan sgbucket.TapEvent
	args    sgbucket.TapArguments
	events  *queue.Queue
}

// Starts a TAP feed on a client connection. The events can be read from the returned channel.
// To stop receiving events, call Close() on the feed.
func (bucket *forestdbBucket) StartTapFeed(args sgbucket.TapArguments) (sgbucket.TapFeed, error) {
	channel := make(chan sgbucket.TapEvent, 10)
	feed := &tapFeedImpl{
		bucket:  bucket,
		channel: channel,
		args:    args,
		events:  queue.NewQueue(),
	}

	if args.Backfill != sgbucket.TapNoBackfill {
		feed.events.Push(&sgbucket.TapEvent{Opcode: sgbucket.TapBeginBackfill})
		if err := bucket.enqueueBackfillEvents(
			args.Backfill,
			args.KeysOnly,
			feed.events); err != nil {
			return nil, err
		}
		feed.events.Push(&sgbucket.TapEvent{Opcode: sgbucket.TapEndBackfill})
	}

	if args.Dump {
		feed.events.Push(nil) // push an eof
	} else {
		// Register the feed with the bucket for future notifications:
		bucket.lock.Lock()
		bucket.tapFeeds = append(bucket.tapFeeds, feed)
		bucket.lock.Unlock()
	}

	go feed.run()

	return feed, nil
}

func (feed *tapFeedImpl) Events() <-chan sgbucket.TapEvent {
	return feed.channel
}

// Closes a TapFeed. Call this if you stop using a TapFeed before its channel ends.
func (feed *tapFeedImpl) Close() error {
	feed.bucket.lock.Lock()
	defer feed.bucket.lock.Unlock()

	for i, afeed := range feed.bucket.tapFeeds {
		if afeed == feed {
			feed.bucket.tapFeeds[i] = nil
		}
	}
	feed.events.Close()
	feed.bucket = nil
	return nil
}

func (feed *tapFeedImpl) run() {
	defer close(feed.channel)
	for {
		event, _ := feed.events.Pull().(*sgbucket.TapEvent)
		if event == nil {
			break
		}
		feed.channel <- *event
	}
}

func (bucket *forestdbBucket) enqueueBackfillEvents(startSequence uint64, keysOnly bool, q *queue.Queue) error {

	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	// create an iterator
	options := forestdb.ITR_NONE
	endSeq := forestdb.SeqNum(0) // is this how to specify no end sequence?
	iterator, err := bucket.kvstore.IteratorSequenceInit(
		forestdb.SeqNum(startSequence),
		endSeq,
		options,
	)
	if err != nil {
		return err
	}

	for {
		doc, err := iterator.Get()
		if err == forestdb.RESULT_ITERATOR_FAIL {
			// we're done, there's nothing in the bucket
			return nil
		}

		if err != nil {
			// unexpected error, return it
			return err
		}
		if doc.Body() != nil && uint64(doc.SeqNum()) >= startSequence {
			event := sgbucket.TapEvent{
				Opcode:   sgbucket.TapMutation,
				Key:      doc.Key(),
				Sequence: uint64(doc.SeqNum()),
			}
			if !keysOnly {
				event.Value = doc.Body()
			}
			q.Push(&event)
		}
		if err := iterator.Next(); err != nil {
			// we're done.  since expected error in this case, don't return an error
			return nil
		}

	}

}

// Caller must have the bucket's RLock, because this method iterates bucket.tapFeeds
func (bucket *forestdbBucket) _postTapEvent(event sgbucket.TapEvent) {
	var eventNoValue sgbucket.TapEvent = event // copies the struct
	eventNoValue.Value = nil

	for _, feed := range bucket.tapFeeds {
		if feed != nil && feed.channel != nil {
			if feed.args.KeysOnly {
				feed.events.Push(&eventNoValue)
			} else {
				feed.events.Push(&event)
			}
		}
	}
}

func (bucket *forestdbBucket) _postTapMutationEvent(key string, value []byte, seq uint64) {

	bucket._postTapEvent(sgbucket.TapEvent{
		Opcode:   sgbucket.TapMutation,
		Key:      []byte(key),
		Value:    copySlice(value),
		Sequence: seq,
	})
}

func (bucket *forestdbBucket) _postTapDeletionEvent(key string, seq uint64) {
	bucket._postTapEvent(sgbucket.TapEvent{
		Opcode:   sgbucket.TapDeletion,
		Key:      []byte(key),
		Sequence: seq,
	})
}
