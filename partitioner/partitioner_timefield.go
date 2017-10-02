package partitioner

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/Shopify/sarama"
	"github.com/UnityTech/kafka-archiver/buffer"
	"github.com/buger/jsonparser"
	"github.com/golang/glog"
)

// TimeFieldPartitioner partitions the data in hourly buckets based on a given timestamp.
// The field must be a unix timestamp in milliseconds.
type TimeFieldPartitioner struct {
	BasePartitioner
	fieldName string
}

// getHourBucket returns the hour based on the timestamp given. This implementation uses
// jsonparser library, which doesn't allocate extra memory.
// The field must be a unix timestamp in milliseconds.
func (t *TimeFieldPartitioner) getHourBucket(msg *sarama.ConsumerMessage) (time.Time, error) {
	ts, err := jsonparser.GetInt(msg.Value, t.fieldName)
	if err != nil {
		return time.Time{}, fmt.Errorf("error getting timestamp from key %q in %q: %v", t.fieldName, msg.Topic, err)
	}

	return time.Unix(ts/1000, 0).UTC().Truncate(1 * time.Hour), nil
}

// GetBuffer returns a buffer that can be used to write the message to.
func (t *TimeFieldPartitioner) GetBuffer(msg *sarama.ConsumerMessage) (*buffer.Buffer, error) {

	// Get the hour Bucket for the given message.
	hourBucket, err := t.getHourBucket(msg)
	if err != nil {
		return nil, err
	}

	// Generate a context with hourBucket.
	ctx := context.WithValue(context.Background(), "hourBucket", hourBucket)

	// Get the buffer.
	buf, err := t.getBuffer(ctx, t.partition(hourBucket, msg), msg)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// partition returns the hash key based on topic, partition and time bucket.
func (t *TimeFieldPartitioner) partition(hourBucket time.Time, msg *sarama.ConsumerMessage) string {
	w := md5.New()

	if _, err := io.WriteString(w, msg.Topic); err != nil {
		glog.Fatal(err)
	}

	if _, err := io.WriteString(w, fmt.Sprintf("%d", msg.Partition)); err != nil {
		glog.Fatal(err)
	}

	if _, err := io.WriteString(w, hourBucket.Format(time.RFC3339)); err != nil {
		glog.Fatal(err)
	}

	return fmt.Sprintf("%x", w.Sum(nil))
}

// GetKey returns the file path used after a file is flushed.
func (t *TimeFieldPartitioner) GetKey(f *buffer.Flush) string {
	hourBucket := f.Ctx.Value("hourBucket").(time.Time)

	return filepath.Join(
		t.getBasePath(f),
		fmt.Sprintf("year=%d/", hourBucket.Year()),
		fmt.Sprintf("month=%02d/", hourBucket.Month()),
		fmt.Sprintf("day=%02d/", hourBucket.Day()),
		fmt.Sprintf("hour=%02d/", hourBucket.Hour()),
		t.getBaseFileName(f),
	)
}
