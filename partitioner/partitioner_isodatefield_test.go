package partitioner

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
)

var _ Partitioner = (*DefaultPartitioner)(nil)

func TestDefaultPartitioner_getHourBucket(t *testing.T) {
	part := New(&Config{Type: "IsoDateFieldPartitioner", FieldKeyName: "ts"}).(*IsoDateFieldPartitioner)

	msg := &sarama.ConsumerMessage{
		Value:     []byte(`{"ts": "2016-07-04T12:45:56Z"}`),
		Topic:     "test",
		Partition: 50,
		Offset:    1000,
	}

	ts, err := part.getHourBucket(msg)
	require.NoError(t, err)
	require.NotNil(t, ts)
	require.False(t, ts.IsZero())
	require.Equal(t, ts.Year(), 2016)
	require.Equal(t, ts.Month(), time.Month(07))
	require.Equal(t, ts.Day(), 04)
	require.Equal(t, ts.Hour(), 12)
	require.Equal(t, ts.Minute(), 00)
	require.Equal(t, ts.Second(), 00)
}
