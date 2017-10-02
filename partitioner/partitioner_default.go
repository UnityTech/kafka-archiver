package partitioner

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"path/filepath"

	"github.com/Shopify/sarama"
	"github.com/UnityTech/kafka-archiver/buffer"
	"github.com/golang/glog"
)

// DefaultPartitioner partitions the data by topic and partition.
type DefaultPartitioner struct {
	BasePartitioner
}

// partition returns a hash key based on topic and partition.
func (d *DefaultPartitioner) partition(msg *sarama.ConsumerMessage) string {
	w := md5.New()

	if _, err := io.WriteString(w, msg.Topic); err != nil {
		glog.Fatal(err)
	}

	if _, err := io.WriteString(w, fmt.Sprintf("%d", msg.Partition)); err != nil {
		glog.Fatal(err)
	}

	return fmt.Sprintf("%x", w.Sum(nil))
}

// GetKey returns the file path used after a file is flushed.
func (d *DefaultPartitioner) GetKey(f *buffer.Flush) string {
	return filepath.Join(
		d.getBasePath(f),
		fmt.Sprintf("partition=%d/", f.Partition),
		d.getBaseFileName(f),
	)
}

// GetBuffer returns a buffer that can be used to write the message to.
func (d *DefaultPartitioner) GetBuffer(msg *sarama.ConsumerMessage) (*buffer.Buffer, error) {
	return d.getBuffer(context.Background(), d.partition(msg), msg)
}
