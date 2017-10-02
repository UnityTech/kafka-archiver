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

// IsoDateFieldPartitioner partitions the data in hourly buckets based on a given timestamp.
// The field must be a unix timestamp in milliseconds.
type IsoDateFieldPartitioner struct {
	BasePartitioner
	fieldName string
}

// getHourBucket returns the hour based on the timestamp given. This implementation uses
// jsonparser library, which doesn't allocate extra memory.
// The field must be a unix timestamp in milliseconds.
func (t *IsoDateFieldPartitioner) getHourBucket(msg *sarama.ConsumerMessage) (time.Time, error) {
	ts, err := jsonparser.GetString(msg.Value, t.fieldName)
	if err != nil {
		return time.Time{}, fmt.Errorf("error getting timestamp from key %q in %q: %v", t.fieldName, msg.Topic, err)
	}

	result, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing timestamp from key %q in %q: %v", t.fieldName, msg.Topic, err)
	}

	return result.UTC().Truncate(1 * time.Hour), nil
}

// GetBuffer returns a buffer that can be used to write the message to.
func (t *IsoDateFieldPartitioner) GetBuffer(msg *sarama.ConsumerMessage) (*buffer.Buffer, error) {

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
func (t *IsoDateFieldPartitioner) partition(hourBucket time.Time, msg *sarama.ConsumerMessage) string {
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
func (t *IsoDateFieldPartitioner) GetKey(f *buffer.Flush) string {
	hourBucket := f.Ctx.Value("hourBucket").(time.Time)

	return filepath.Join(
		t.getBasePath(f),
		fmt.Sprintf(
			"date=%d-%02d-%02d/",
			hourBucket.Year(),
			hourBucket.Month(),
			hourBucket.Day(),
		),
		fmt.Sprintf("hour=%02d/", hourBucket.Hour()),
		t.getBaseFileName(f),
	)
}
