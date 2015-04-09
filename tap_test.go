package forestbucket

import (
	"os"
	"testing"

	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/walrus"
)

// Disabled since the backfill stuff is not implemented yet
func TestBackfill(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	bucket.Add("able", 0, "A")
	bucket.Add("baker", 0, "B")
	bucket.Add("charlie", 0, "C")

	feed, err := bucket.StartTapFeed(walrus.TapArguments{Backfill: 0, Dump: true})
	assertNoError(t, err, "StartTapFeed failed")
	assert.True(t, feed != nil)

	event := <-feed.Events()
	assert.Equals(t, event.Opcode, walrus.TapBeginBackfill)
	results := map[string]string{}
	for i := 0; i < 3; i++ {
		event := <-feed.Events()
		assert.Equals(t, event.Opcode, walrus.TapMutation)
		results[string(event.Key)] = string(event.Value)
	}
	assert.DeepEquals(t, results, map[string]string{
		"able": `"A"`, "baker": `"B"`, "charlie": `"C"`})

	event = <-feed.Events()
	assert.Equals(t, event.Opcode, walrus.TapEndBackfill)

	event, ok := <-feed.Events()
	assert.False(t, ok)
}

func TestMutations(t *testing.T) {

	bucket, tempDir := GetTestBucket()

	defer os.RemoveAll(tempDir)
	defer CloseBucket(bucket)

	bucket.Add("able", 0, "A")
	bucket.Add("baker", 0, "B")
	bucket.Add("charlie", 0, "C")

	feed, err := bucket.StartTapFeed(walrus.TapArguments{Backfill: walrus.TapNoBackfill})
	assertNoError(t, err, "StartTapFeed failed")
	assert.True(t, feed != nil)
	defer feed.Close()

	bucket.Add("delta", 0, "D")
	bucket.Add("eskimo", 0, "E")

	go func() {
		bucket.Add("fahrvergnügen", 0, "F")
		bucket.Delete("eskimo")
	}()

	assert.DeepEquals(t, <-feed.Events(), walrus.TapEvent{Opcode: walrus.TapMutation, Key: []byte("delta"), Value: []byte(`"D"`), Sequence: 4})
	assert.DeepEquals(t, <-feed.Events(), walrus.TapEvent{Opcode: walrus.TapMutation, Key: []byte("eskimo"), Value: []byte(`"E"`), Sequence: 5})
	assert.DeepEquals(t, <-feed.Events(), walrus.TapEvent{Opcode: walrus.TapMutation, Key: []byte("fahrvergnügen"), Value: []byte(`"F"`), Sequence: 6})

	assert.DeepEquals(t, <-feed.Events(), walrus.TapEvent{Opcode: walrus.TapDeletion, Key: []byte("eskimo"), Sequence: 7})

}
