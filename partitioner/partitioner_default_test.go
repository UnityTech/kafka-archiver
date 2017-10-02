package partitioner

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
)

var _ Partitioner = (*DefaultPartitioner)(nil)

func TestDefaultPartitioner_partition(t *testing.T) {
	part := New(&Config{Type: "DefaultPartitioner"}).(*DefaultPartitioner)

	msg := &sarama.ConsumerMessage{
		Value:     []byte("json"),
		Topic:     "test",
		Partition: 50,
		Offset:    1000,
	}

	require.Equal(t, "e4abf54583b16ef4d4ba714617038d54", part.partition(msg))
}
