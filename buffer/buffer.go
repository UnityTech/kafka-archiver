package buffer

// Adapted from https://github.com/tj/go-disk-buffer

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	gzip "github.com/klauspost/pgzip"
)

// PID for unique filename.
var pid = os.Getpid()

// Ids for unique filename.
var ids = int64(0)

// Reason for flush.
type Reason string

// Flush reasons.
const (
	Forced   Reason = "forced"
	Writes   Reason = "writes"
	Bytes    Reason = "bytes"
	Interval Reason = "interval"
)

// Flush represents a flushed file.
type Flush struct {
	Reason     Reason
	Path       string
	Writes     int64
	Bytes      int64
	Opened     time.Time
	Closed     time.Time
	Age        time.Duration
	Topic      string
	Partition  int32
	LastOffset int64
	Ctx        context.Context
}

// Config for disk buffer.
type Config struct {
	TempDir       string        // Where to store temporary files
	FlushWrites   int64         // Flush after N writes, zero to disable
	FlushBytes    int64         // Flush after N bytes, zero to disable
	FlushInterval time.Duration // Flush after duration, zero to disable
	BufferSize    int           // Buffer size for writes
	Queue         chan *Flush   // Queue of flushed files
}

// Validate the configuration.
func (c *Config) Validate() error {
	switch {
	case c.FlushBytes == 0 && c.FlushWrites == 0 && c.FlushInterval == 0:
		return fmt.Errorf("at least one flush mechanism must be non-zero")
	default:
		return nil
	}
}

// Buffer represents a 1:N on-disk buffer.
type Buffer struct {
	*Config

	topic      string
	partition  int32
	lastOffset int64
	ctx        context.Context

	path string
	ids  int64
	id   int64

	sync.RWMutex
	buf    *bufio.Writer
	opened time.Time
	writes int64
	bytes  int64
	gzf    *gzip.Writer
	file   *os.File
	tick   *time.Ticker
}

// New buffer for topic and partition. The path given is used for the base
// of the filenames created, which append ".{pid}.{id}.{fid}".
func New(ctx context.Context, topic string, partition int32, config *Config) (*Buffer, error) {
	id := atomic.AddInt64(&ids, 1)

	path, err := ioutil.TempDir(config.TempDir, fmt.Sprintf("buffer-%s-%d-", topic, partition))
	if err != nil {
		return nil, err
	}

	b := &Buffer{
		Config:     config,
		topic:      topic,
		partition:  partition,
		path:       path,
		id:         id,
		lastOffset: math.MinInt64,
		ctx:        ctx,
	}

	if b.Queue == nil {
		b.Queue = make(chan *Flush)
	}

	if b.FlushInterval != 0 {
		b.tick = time.NewTicker(config.FlushInterval)
		go b.loop()
	}

	err = config.Validate()
	if err != nil {
		return nil, err
	}

	b.Lock()
	defer b.Unlock()
	return b, b.open()
}

// Write implements io.Writer.
func (b *Buffer) Write(data []byte) (int, error) {
	glog.V(8).Infof("write %s", data)

	b.Lock()
	defer b.Unlock()

	n, err := b.write(data)
	if err != nil {
		return n, err
	}

	if b.FlushWrites != 0 && b.writes >= b.FlushWrites {
		err := b.flush(Writes)
		if err != nil {
			return n, err
		}
	}

	if b.FlushBytes != 0 && b.bytes >= b.FlushBytes {
		err := b.flush(Bytes)
		if err != nil {
			return n, err
		}
	}

	return n, err
}

func (b *Buffer) WriteMessage(msg *sarama.ConsumerMessage) (int, error) {
	b.RLock()
	bufTopic := b.topic
	bufPartition := b.partition
	b.RUnlock()

	if msg.Topic != bufTopic {
		return 0, fmt.Errorf("error message topic doesn't match, got %s want %s", msg.Topic, bufTopic)
	}

	if msg.Partition != bufPartition {
		return 0, fmt.Errorf("error message partition doesn't match, got %d want %d", msg.Partition, bufPartition)
	}

	data := msg.Value
	if !bytes.HasSuffix(data, []byte{'\n'}) {
		data = append(data, '\n')
	}

	n, err := b.Write(data)
	if err == nil {
		b.RLock()
		if b.lastOffset < msg.Offset {
			glog.V(5).Infof("marking internal offset for topic partition, %s[%d] = %d", msg.Topic, msg.Partition, msg.Offset)
			b.lastOffset = msg.Offset
		}
		b.RUnlock()
	}

	return n, err
}

func (b *Buffer) Topic() string {
	return b.topic
}

func (b *Buffer) Partition() int32 {
	return b.partition
}

func (b *Buffer) Age() time.Duration {
	return time.Now().Sub(b.opened)
}

// Close the underlying file after flushing.
func (b *Buffer) Close() error {
	b.Lock()
	defer b.Unlock()

	if b.tick != nil {
		b.tick.Stop()
	}

	return b.flush(Forced)
}

// Flush forces a flush.
func (b *Buffer) Flush() error {
	b.Lock()
	defer b.Unlock()
	return b.flush(Forced)
}

// Writes returns the number of writes made to the current file.
func (b *Buffer) Writes() int64 {
	b.RLock()
	defer b.RUnlock()
	return b.writes
}

// Bytes returns the number of bytes made to the current file.
func (b *Buffer) Bytes() int64 {
	b.RLock()
	defer b.RUnlock()
	return b.bytes
}

// Loop for flush interval.
func (b *Buffer) loop() {
	for range b.tick.C {
		b.Lock()
		if err := b.flush(Interval); err != nil {
			glog.Fatal(err)
		}
		b.Unlock()
	}
}

// Open a new buffer.
func (b *Buffer) open() error {
	path := b.pathname()

	glog.V(2).Infof("opening %q", path)
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	gzw, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		return err
	}

	// Override default concurrency settings. Use 4 concurrent blocks.
	if err := gzw.SetConcurrency(250000, 4); err != nil {
		glog.Fatal("Gzip configuration issue: ", err)
	}

	glog.V(8).Infof("buffer size %d", b.BufferSize)
	if b.BufferSize != 0 {
		b.buf = bufio.NewWriterSize(gzw, b.BufferSize)
	}

	glog.V(8).Infof("reset state")
	b.opened = time.Now()
	b.lastOffset = math.MinInt64
	b.writes = 0
	b.bytes = 0
	b.file = f
	b.gzf = gzw

	return nil
}

// Write with metrics.
func (b *Buffer) write(data []byte) (int, error) {
	b.writes++
	b.bytes += int64(len(data))

	if b.BufferSize != 0 {
		return b.buf.Write(data)
	}

	return b.gzf.Write(data)
}

// Flush for the given reason and re-open.
func (b *Buffer) flush(reason Reason) error {
	if b.writes == 0 {
		glog.V(6).Info("nothing to flush")
		return nil
	}

	glog.V(4).Infof("flushing %q (reason: %s)", b.file.Name(), reason)
	err := b.close()
	if err != nil {
		return err
	}

	b.Queue <- &Flush{
		Reason:     reason,
		Writes:     b.writes,
		Bytes:      b.bytes,
		Opened:     b.opened,
		Closed:     time.Now(),
		Path:       b.file.Name() + ".closed",
		Age:        time.Since(b.opened),
		Topic:      b.topic,
		Partition:  b.partition,
		LastOffset: b.lastOffset,
		Ctx:        b.ctx,
	}

	return b.open()
}

// Close existing file after a rename.
func (b *Buffer) close() error {
	if b.file == nil {
		return nil
	}

	path := b.file.Name()

	glog.V(5).Infof("renaming %q", path)
	err := os.Rename(path, path+".closed")
	if err != nil {
		return err
	}

	if b.BufferSize != 0 {
		err = b.buf.Flush()
		if err != nil {
			return err
		}
	}

	if err := b.gzf.Flush(); err != nil {
		return err
	}

	if err := b.gzf.Close(); err != nil {
		return err
	}

	if err := b.file.Sync(); err != nil {
		return err
	}

	glog.V(3).Infof("closing %q", path)
	return b.file.Close()
}

// Pathname for a new buffer.
func (b *Buffer) pathname() string {
	fid := atomic.AddInt64(&b.ids, 1)
	return fmt.Sprintf("%s.%d.%d.%d", b.path, pid, b.id, fid)
}
