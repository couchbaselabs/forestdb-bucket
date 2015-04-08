package forestbucket

import (
	"github.com/couchbaselabs/walrus"
	"github.com/tleyden/go-safe-dstruct/queue"
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
		bucket.enqueueBackfillEvents(args.Backfill, args.KeysOnly, feed.events)
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

func (bucket *forestdbBucket) enqueueBackfillEvents(startSequence uint64, keysOnly bool, q *queue.Queue) {
	panic("enqueueBackfillEvents not implemented yet")

}
