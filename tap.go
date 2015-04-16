package forestbucket

import (
	"github.com/couchbaselabs/go-safe-dstruct/queue"
	"github.com/couchbaselabs/goforestdb"
	"github.com/couchbaselabs/walrus"
)

type tapFeedImpl struct {
	bucket  *forestdbBucket
	channel chan walrus.TapEvent
	args    walrus.TapArguments
	events  *queue.Queue
}

// Starts a TAP feed on a client connection. The events can be read from the returned channel.
// To stop receiving events, call Close() on the feed.
func (bucket *forestdbBucket) StartTapFeed(args walrus.TapArguments) (walrus.TapFeed, error) {
	channel := make(chan walrus.TapEvent, 10)
	feed := &tapFeedImpl{
		bucket:  bucket,
		channel: channel,
		args:    args,
		events:  queue.NewQueue(),
	}

	if args.Backfill != walrus.TapNoBackfill {
		feed.events.Push(&walrus.TapEvent{Opcode: walrus.TapBeginBackfill})
		if err := bucket.enqueueBackfillEvents(
			args.Backfill,
			args.KeysOnly,
			feed.events); err != nil {
			return nil, err
		}
		feed.events.Push(&walrus.TapEvent{Opcode: walrus.TapEndBackfill})
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

func (feed *tapFeedImpl) Events() <-chan walrus.TapEvent {
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
		event, _ := feed.events.Pull().(*walrus.TapEvent)
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
		if err != nil {
			return err
		}
		if doc.Body() != nil && uint64(doc.SeqNum()) >= startSequence {
			event := walrus.TapEvent{
				Opcode:   walrus.TapMutation,
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
func (bucket *forestdbBucket) _postTapEvent(event walrus.TapEvent) {
	var eventNoValue walrus.TapEvent = event // copies the struct
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

	bucket._postTapEvent(walrus.TapEvent{
		Opcode:   walrus.TapMutation,
		Key:      []byte(key),
		Value:    copySlice(value),
		Sequence: seq,
	})
}

func (bucket *forestdbBucket) _postTapDeletionEvent(key string, seq uint64) {
	bucket._postTapEvent(walrus.TapEvent{
		Opcode:   walrus.TapDeletion,
		Key:      []byte(key),
		Sequence: seq,
	})
}
