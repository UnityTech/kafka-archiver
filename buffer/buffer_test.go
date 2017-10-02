package buffer

// Adapted from https://github.com/tj/go-disk-buffer

import (
	"compress/gzip"
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
)

var config = &Config{
	Queue:         make(chan *Flush, 100),
	FlushWrites:   1000,
	FlushBytes:    1000,
	FlushInterval: time.Second,
}

func discard(b *Buffer) {
	go func() {
		for range b.Queue {

		}
	}()
}

func write(buffer *Buffer, n int, b []byte) {
	go func() {
		for i := 0; i < n; i++ {
			_, err := buffer.Write(b)
			if err != nil {
				panic(err)
			}
		}
	}()
}

// Test immediate open / close.
func TestBuffer_Open(t *testing.T) {
	b, err := New(context.Background(), "test-topic", 0, config)
	require.Equal(t, nil, err)

	err = b.Close()
	require.Equal(t, nil, err)
}

// Test buffer writes.
func TestBuffer_Write(t *testing.T) {
	b, err := New(context.Background(), "test-topic", 0, config)
	require.Equal(t, nil, err)

	n, err := b.Write([]byte("hello"))
	require.Equal(t, nil, err)
	require.Equal(t, 5, n)
	require.Equal(t, int64(1), b.writes)
	require.Equal(t, int64(5), b.bytes)

	n, err = b.Write([]byte("world"))
	require.Equal(t, nil, err)
	require.Equal(t, 5, n)
	require.Equal(t, int64(2), b.writes)
	require.Equal(t, int64(10), b.bytes)

	err = b.Close()
	require.Equal(t, nil, err)
}

// Test buffer writes.
func TestBuffer_WriteMessage(t *testing.T) {
	ch := make(chan *Flush, 1)
	b, err := New(context.Background(), "test-topic", 0, &Config{
		Queue:         ch,
		FlushWrites:   10,
		FlushBytes:    1024,
		FlushInterval: time.Second,
	})
	require.Equal(t, nil, err)

	msg0 := &sarama.ConsumerMessage{
		Key:       []byte("key"),
		Value:     []byte(`{"test": "json", "ts": 0}`),
		Topic:     "test-topic",
		Partition: 0,
	}

	n, err := b.WriteMessage(msg0)
	require.NoError(t, err)
	require.Equal(t, len(msg0.Value)+1, n)
	require.Equal(t, int64(1), b.writes)
	require.Equal(t, int64(len(msg0.Value)+1), b.bytes)

	msg1 := &sarama.ConsumerMessage{
		Key:       []byte("key"),
		Value:     []byte(`{"test_2": "json_2", "ts": 3599}`),
		Topic:     "test-topic",
		Partition: 0,
	}

	n, err = b.WriteMessage(msg1)
	require.NoError(t, err)
	require.Equal(t, len(msg1.Value)+1, n)
	require.Equal(t, int64(2), b.writes)
	require.Equal(t, int64(len(msg0.Value)+len(msg1.Value)+2), b.bytes)

	err = b.Close()
	require.Equal(t, nil, err)

	flushedFile := <-ch
	require.NotNil(t, flushedFile)

	// test that the file is gzip
	file, err := os.Open(flushedFile.Path)
	require.NoError(t, err)

	gzr, err := gzip.NewReader(file)
	require.NoError(t, err)

	fileBytes, err := ioutil.ReadAll(gzr)
	require.NoError(t, err)

	require.Equal(t, "{\"test\": \"json\", \"ts\": 0}\n{\"test_2\": \"json_2\", \"ts\": 3599}\n", string(fileBytes))
}

// Test flushing on write count.
func TestBuffer_Write_FlushOnWrites(t *testing.T) {
	b, err := New(context.Background(), "test-topic", 0, &Config{
		Queue:         make(chan *Flush, 100),
		FlushWrites:   10,
		FlushBytes:    1024,
		FlushInterval: time.Second,
	})

	require.Equal(t, nil, err)

	write(b, 25, []byte("hello"))

	flush := <-b.Queue
	require.Equal(t, int64(10), flush.Writes)
	require.Equal(t, int64(50), flush.Bytes)
	require.Equal(t, Writes, flush.Reason)

	flush = <-b.Queue
	require.Equal(t, int64(10), flush.Writes)
	require.Equal(t, int64(50), flush.Bytes)
	require.Equal(t, Writes, flush.Reason)

	err = b.Close()
	require.Equal(t, nil, err)
}

// Test flushing on byte count.
func TestBuffer_Write_FlushOnBytes(t *testing.T) {
	b, err := New(context.Background(), "test-topic", 0, &Config{
		Queue:         make(chan *Flush, 100),
		FlushWrites:   10000,
		FlushBytes:    1024,
		FlushInterval: time.Second,
	})

	require.Equal(t, nil, err)

	write(b, 250, []byte("hello world"))
	flush := <-b.Queue
	require.Equal(t, int64(94), flush.Writes)
	require.Equal(t, int64(1034), flush.Bytes)
	require.Equal(t, Bytes, flush.Reason)

	flush = <-b.Queue
	require.Equal(t, int64(94), flush.Writes)
	require.Equal(t, int64(1034), flush.Bytes)
	require.Equal(t, Bytes, flush.Reason)

	err = b.Close()
	require.Equal(t, nil, err)
}

// Test flushing on interval.
func TestBuffer_Write_FlushOnInterval(t *testing.T) {
	b, err := New(context.Background(), "test-topic", 0, &Config{
		Queue:         make(chan *Flush, 100),
		FlushInterval: time.Second,
	})

	require.Equal(t, nil, err)

	b.Write([]byte("hello world"))
	b.Write([]byte("hello world"))

	flush := <-b.Queue
	require.Equal(t, int64(2), flush.Writes)
	require.Equal(t, int64(22), flush.Bytes)
	require.Equal(t, Interval, flush.Reason)

	err = b.Close()
	require.Equal(t, nil, err)
}

// Test config validation.
func TestConfig_Validate(t *testing.T) {
	_, err := New(context.Background(), "test-topic", 0, &Config{})
	require.Equal(t, "at least one flush mechanism must be non-zero", err.Error())
}

// Benchmark buffer writes.
func BenchmarkBuffer_Write(t *testing.B) {
	b, err := New(context.Background(), "test-topic", 0, &Config{
		FlushBytes:    1 << 30,
		FlushInterval: time.Minute,
	})

	if err != nil {
		t.Fatalf("error: %s", err)
	}

	discard(b)

	t.ResetTimer()

	t.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			b.Write([]byte(testData[(i % (len(testData) - 1))]))
			i++
		}
	})
}

// Benchmark buffer writes with bufio.
func BenchmarkBuffer_Write_Bufio1KiB(t *testing.B) {
	b, err := New(context.Background(), "test-topic", 0, &Config{
		FlushBytes:    1 << 30,
		FlushInterval: time.Minute,
		BufferSize:    1 << 10,
	})

	if err != nil {
		t.Fatalf("error: %s", err)
	}

	discard(b)

	t.ResetTimer()

	t.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			b.Write([]byte(testData[(i % (len(testData) - 1))]))
			i++
		}
	})
}

// Benchmark buffer writes with bufio.
func BenchmarkBuffer_Write_Bufio8KiB(t *testing.B) {
	b, err := New(context.Background(), "test-topic", 0, &Config{
		FlushBytes:    1 << 30,
		FlushInterval: time.Minute,
		BufferSize:    8 << 10,
	})

	if err != nil {
		t.Fatalf("error: %s", err)
	}

	discard(b)

	t.ResetTimer()

	t.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			b.Write([]byte(testData[(i % (len(testData) - 1))]))
			i++
		}
	})
}

// Benchmark buffer writes with bufio.
func BenchmarkBuffer_Write_Bufio32KiB(t *testing.B) {
	b, err := New(context.Background(), "test-topic", 0, &Config{
		FlushBytes:    1 << 30,
		FlushInterval: time.Minute,
		BufferSize:    32 << 10,
	})

	if err != nil {
		t.Fatalf("error: %s", err)
	}

	discard(b)

	t.ResetTimer()

	t.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			b.Write([]byte(testData[(i % (len(testData) - 1))]))
			i++
		}
	})
}

// Benchmark buffer writes with bufio.
func BenchmarkBuffer_Write_Bufio128KiB(t *testing.B) {
	b, err := New(context.Background(), "test-topic", 0, &Config{
		FlushBytes:    1 << 30,
		FlushInterval: time.Minute,
		BufferSize:    128 << 10,
	})

	if err != nil {
		t.Fatalf("error: %s", err)
	}

	discard(b)

	t.ResetTimer()

	t.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			b.Write([]byte(testData[(i % (len(testData) - 1))]))
			i++
		}
	})
}
