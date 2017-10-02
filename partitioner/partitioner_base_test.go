package partitioner

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/UnityTech/kafka-archiver/buffer"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/stretchr/testify/require"
)

func TestBasePartitioner_getBasePath(t *testing.T) {
	b := new(BasePartitioner)
	f := &buffer.Flush{
		Topic:     "test",
		Partition: 0,
	}

	require.Equal(t, "/test", b.getBasePath(f))
}

func TestBasePartitioner_getBasePathWithBaseBath(t *testing.T) {
	b := new(BasePartitioner)
	b.pathBaseFolder = "backup"
	f := &buffer.Flush{
		Topic:     "test",
		Partition: 0,
	}

	require.Equal(t, "backup/test", b.getBasePath(f))
}

func TestBasePartitioner_getBasePathWithBaseBathAndTopicPrefix(t *testing.T) {
	b := new(BasePartitioner)
	b.pathBaseFolder = "backup"
	b.pathTopicNamePrefix = "topic="
	f := &buffer.Flush{
		Topic:     "test",
		Partition: 0,
	}

	require.Equal(t, "backup/topic=test", b.getBasePath(f))
}

func TestBasePartitioner_getBaseFileName(t *testing.T) {
	b := new(BasePartitioner)
	f := &buffer.Flush{
		Topic:      "test",
		Partition:  50,
		LastOffset: 1000,
	}

	require.Regexp(t, regexp.MustCompile("0050.1000.[\\w]{8}-[\\w]{4}-[\\w]{4}-[\\w]{4}-[\\w]{12}.gz"), b.getBaseFileName(f))
}

func TestBasePartitioner_getBuffer(t *testing.T) {
	b := new(BasePartitioner)
	b.subscriptions = make(map[string]*buffer.Buffer)
	b.defaultBufferConfig = &buffer.Config{
		Queue:         make(chan *buffer.Flush, 100),
		FlushWrites:   1000,
		FlushBytes:    1000,
		FlushInterval: time.Second,
	}
	msg := &sarama.ConsumerMessage{
		Value:     []byte("json"),
		Topic:     "test",
		Partition: 50,
		Offset:    1000,
	}

	buf, err := b.getBuffer(context.Background(), "test50", msg)
	defer buf.Close()
	require.NoError(t, err)

	n, err := buf.WriteMessage(msg)
	require.Equal(t, 5, n)
	require.NoError(t, err)

	b.RLock()
	require.Contains(t, b.subscriptions, "test50")
	require.NotNil(t, b.subscriptions["test50"])
	require.Equal(t, b.subscriptions["test50"], buf)
	require.EqualValues(t, 5, b.subscriptions["test50"].Bytes())
	require.EqualValues(t, 1, b.subscriptions["test50"].Writes())
	b.RUnlock()
}

func TestBasePartitioner_cleanup(t *testing.T) {
	defaultPartitionerCleanup = 50 * time.Millisecond
	defaultPartitionerTimeout = 100 * time.Millisecond

	b := new(BasePartitioner)
	b.subscriptions = make(map[string]*buffer.Buffer)
	b.defaultBufferConfig = &buffer.Config{
		Queue:         make(chan *buffer.Flush, 100),
		FlushWrites:   1000,
		FlushBytes:    1000,
		FlushInterval: 100 * time.Millisecond,
	}
	go b.cleanup()

	msg := &sarama.ConsumerMessage{
		Value:     []byte("json"),
		Topic:     "test",
		Partition: 50,
		Offset:    1000,
	}

	buf, err := b.getBuffer(context.Background(), "test50", msg)
	require.NoError(t, err)

	n, err := buf.WriteMessage(msg)
	require.Equal(t, 5, n)
	require.NoError(t, err)

	time.Sleep(defaultPartitionerCleanup*2 + 2*defaultPartitionerTimeout)

	b.RLock()
	require.NotContains(t, b.subscriptions, "test50")
	b.RUnlock()
}

func TestBasePartitioner_Rebalance(t *testing.T) {
	config := &buffer.Config{
		Queue:         make(chan *buffer.Flush, 100),
		FlushWrites:   1000,
		FlushBytes:    1000,
		FlushInterval: time.Second,
	}

	b := new(BasePartitioner)
	b.subscriptions = make(map[string]*buffer.Buffer)
	b.Lock()
	b.subscriptions["buf1"], _ = buffer.New(context.Background(), "raw", 0, config)
	b.subscriptions["buf2"], _ = buffer.New(context.Background(), "raw", 1, config)
	b.subscriptions["buf3"], _ = buffer.New(context.Background(), "released", 2, config)
	b.subscriptions["buf4"], _ = buffer.New(context.Background(), "released", 50, config)
	b.Unlock()

	n := &cluster.Notification{
		Released: map[string][]int32{
			"raw":      []int32{},
			"released": []int32{50, 2},
		},
	}

	b.Rebalance(n)

	b.RLock()
	require.Contains(t, b.subscriptions, "buf1")
	require.Contains(t, b.subscriptions, "buf2")
	require.NotContains(t, b.subscriptions, "buf3")
	require.NotContains(t, b.subscriptions, "buf4")
	b.RUnlock()

}
