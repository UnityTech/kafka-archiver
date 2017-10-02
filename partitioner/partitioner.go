package partitioner

import (
	"github.com/Shopify/sarama"
	"github.com/UnityTech/kafka-archiver/buffer"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/golang/glog"
)

// Config for partitioner.
type Config struct {
	Type                string // Type of the selected partitioner.
	BaseFolder          string
	TopicNamePrefix     string
	FieldKeyName        string
	DefaultBufferConfig *buffer.Config
}

// Partitioner.
type Partitioner interface {
	// GetBuffer returns a buffer that can be used to write the message to.
	// The buffer is decided based on the Partitioner class used.
	GetBuffer(msg *sarama.ConsumerMessage) (*buffer.Buffer, error)

	// GetKey returns the file path used after a file is flushed.
	GetKey(f *buffer.Flush) string

	// Rebalance is called when the consumer group rebalances the topics and
	// partitions among the cluster. This method holds the lock until it finishes looping
	// through all the consumers.
	Rebalance(n *cluster.Notification)

	// Close takes care of closing all the buffers.
	Close()
}

// NewPartitioner creates a new object that satisfies the Partitioner interface.
// Uses a Worker instance for additional configuration fields.
func New(c *Config) Partitioner {
	switch c.Type {
	case "DefaultPartitioner":
		p := new(DefaultPartitioner)
		p.subscriptions = make(map[string]*buffer.Buffer)
		p.defaultBufferConfig = c.DefaultBufferConfig
		p.pathBaseFolder = c.BaseFolder
		p.pathTopicNamePrefix = c.TopicNamePrefix
		go p.cleanup()
		return p
	case "TimeFieldPartitioner":
		p := new(TimeFieldPartitioner)
		p.subscriptions = make(map[string]*buffer.Buffer)
		p.defaultBufferConfig = c.DefaultBufferConfig
		p.pathBaseFolder = c.BaseFolder
		p.pathTopicNamePrefix = c.TopicNamePrefix
		p.fieldName = c.FieldKeyName
		if p.fieldName == "" {
			glog.Fatal("TimeFieldPartitioner requires partitioner-key flag")
		}
		go p.cleanup()
		return p
	case "IsoDateFieldPartitioner":
		p := new(IsoDateFieldPartitioner)
		p.subscriptions = make(map[string]*buffer.Buffer)
		p.defaultBufferConfig = c.DefaultBufferConfig
		p.pathBaseFolder = c.BaseFolder
		p.pathTopicNamePrefix = c.TopicNamePrefix
		p.fieldName = c.FieldKeyName
		if p.fieldName == "" {
			glog.Fatal("IsoDateFieldPartitioner requires partitioner-key flag")
		}
		go p.cleanup()
		return p
	default:
		glog.Fatalf("unknown partitioner key %s", c.Type)
	}

	return nil
}
