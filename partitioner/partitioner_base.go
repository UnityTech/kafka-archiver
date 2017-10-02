package partitioner

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/UnityTech/kafka-archiver/buffer"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/golang/glog"
	"github.com/google/uuid"
)

var (
	defaultPartitionerCleanup = 5 * time.Minute
	defaultPartitionerTimeout = 10 * time.Minute
)

// BasePartitioner serves as a base struct for every Partitioner instance.
type BasePartitioner struct {
	sync.RWMutex
	subscriptions       map[string]*buffer.Buffer
	pathBaseFolder      string
	pathTopicNamePrefix string
	defaultBufferConfig *buffer.Config
}

// getBasePath returns the base path for Cloud Storage Services.
func (b *BasePartitioner) getBasePath(f *buffer.Flush) string {
	return filepath.Join(
		fmt.Sprintf("%s/", b.pathBaseFolder),
		fmt.Sprintf("%s%s/", b.pathTopicNamePrefix, f.Topic),
	)
}

// getBaseFileName returns the base file name.
func (b *BasePartitioner) getBaseFileName(f *buffer.Flush) string {
	return fmt.Sprintf("%04d.%d.%s.gz", f.Partition, f.LastOffset, uuid.New().String())
}

// checkCleanupBuffers closes the buffer if it been opened for more than defaultPartitionerTimeout
// and has no writes.
func (b *BasePartitioner) checkCleanupBuffer(buf *buffer.Buffer) bool {
	buf.RLock()
	bufAge := buf.Age()
	buf.RUnlock()

	if bufAge > defaultPartitionerTimeout && buf.Writes() == 0 {
		if err := buf.Close(); err != nil {
			glog.Error(err)
		}
		return true
	}

	return false
}

// cleanup runs on a scheduled timer (defaultPartitionerCleanup), it locks while looks for expired or old buffers.
func (b *BasePartitioner) cleanup() {
	for {
		time.Sleep(defaultPartitionerCleanup)
		b.Lock()

		for key, buf := range b.subscriptions {
			if b.checkCleanupBuffer(buf) {
				delete(b.subscriptions, key)
			}
		}
		b.Unlock()
	}
}

// Rebalance is called when the consumer group rebalances the topics and
// partitions among the cluster. This method holds the lock until it finishes looping
// through all the consumers.
func (b *BasePartitioner) Rebalance(n *cluster.Notification) {
	b.Lock()
	defer b.Unlock()

	for key, buf := range b.subscriptions {
		if relpar, ok := n.Released[buf.Topic()]; ok {
			for _, p := range relpar {
				if p == buf.Partition() {
					if err := buf.Close(); err != nil {
						glog.Fatal(err)
					}

					delete(b.subscriptions, key)
					break
				}
			}
		}
	}
}

// Close closes all the current subscriptions buffers.
func (b *BasePartitioner) Close() {
	b.Lock()
	defer b.Unlock()

	for key, buf := range b.subscriptions {
		if err := buf.Close(); err != nil {
			glog.Error(err)
		}

		delete(b.subscriptions, key)
	}
}

// getBuffer returns or creates a buffer for a given hashing key. The hash key is given
// by the Partitioner implementation.
func (b *BasePartitioner) getBuffer(ctx context.Context, hKey string, msg *sarama.ConsumerMessage) (*buffer.Buffer, error) {
	b.Lock()
	defer b.Unlock()

	buf, ok := b.subscriptions[hKey]
	if !ok {
		var err error
		glog.V(6).Infof("opening new buffer for topic=%s partition=%d hash=%s", msg.Topic, msg.Partition, hKey)
		buf, err = buffer.New(ctx, msg.Topic, msg.Partition, b.defaultBufferConfig)
		if err != nil {
			return nil, err
		}

		b.subscriptions[hKey] = buf
	}

	return buf, nil
}
