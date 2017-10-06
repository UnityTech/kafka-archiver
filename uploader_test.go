package main

import (
	"context"
	"io/ioutil"
	"testing"
	"time"

	"github.com/UnityTech/kafka-archiver/buffer"
	"github.com/UnityTech/kafka-archiver/partitioner"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/require"
)

func newTestWorker(partitioner partitioner.Partitioner, s3Client *s3.S3) *Worker {
	w := NewWorker()
	w.s3enabled = true
	w.s3Concurrency = 1
	w.partitioner = partitioner
	w.s3bucket = "test"
	w.uploader = s3manager.NewUploaderWithClient(s3Client)
	return w
}

func TestUploadWithDefaultPartitioner(t *testing.T) {
	s3ClientMock := s3.New(session.New())
	s3ClientMock.Handlers.Clear()
	s3ClientMock.Handlers.Send.PushBack(func(r *request.Request) {
		switch x := r.Params.(type) {
		case *s3.PutObjectInput:
			bt, err := ioutil.ReadAll(x.Body)
			require.NoError(t, err)
			require.Equal(t, "{\"message\": 1}{\"message\": 2}{\"message\": 3}", string(bt))
			require.Contains(t, *x.Key, "/testTopic1/partition=10/0010.5555.")
			require.Equal(t, *x.Bucket, "test")
		}
	})

	// Create new temporary file to upload.
	f, err := ioutil.TempFile("", "kafka-archiver-upload-test")
	require.NoError(t, err)
	f.WriteString(`{"message": 1}`)
	f.WriteString(`{"message": 2}`)
	f.WriteString(`{"message": 3}`)
	require.NoError(t, f.Close())

	// Create worker and upload.
	w := newTestWorker(partitioner.New(&partitioner.Config{Type: "DefaultPartitioner"}), s3ClientMock)
	w.upload(&buffer.Flush{
		Topic:      "testTopic1",
		Path:       f.Name(),
		Partition:  10,
		LastOffset: 5555,
	})
}

func TestUploadWithTimeFieldPartitioner(t *testing.T) {
	s3ClientMock := s3.New(session.New())
	s3ClientMock.Handlers.Clear()
	s3ClientMock.Handlers.Send.PushBack(func(r *request.Request) {
		switch x := r.Params.(type) {
		case *s3.PutObjectInput:
			bt, err := ioutil.ReadAll(x.Body)
			require.NoError(t, err)
			require.Equal(t, "{\"message\": 1, \"ts\": 1444262400}{\"message\": 2, \"ts\": 1444262401}{\"message\": 3, \"ts\": 1444262402}", string(bt))
			require.Contains(t, *x.Key, "backup/topic=testTopic2/year=2015/month=10/day=08/hour=00/0010.808.")
			require.Equal(t, *x.Bucket, "test")
		}
	})

	// Create new temporary file to upload.
	f, err := ioutil.TempFile("", "kafka-archiver-upload-test")
	require.NoError(t, err)
	f.WriteString(`{"message": 1, "ts": 1444262400}`)
	f.WriteString(`{"message": 2, "ts": 1444262401}`)
	f.WriteString(`{"message": 3, "ts": 1444262402}`)
	require.NoError(t, f.Close())

	w := newTestWorker(partitioner.New(&partitioner.Config{
		Type:            "TimeFieldPartitioner",
		FieldKeyName:    "ts",
		TopicNamePrefix: "topic=",
		BaseFolder:      "backup",
	}), s3ClientMock)

	w.upload(&buffer.Flush{
		Topic:      "testTopic2",
		Path:       f.Name(),
		Partition:  10,
		LastOffset: 808,
		Ctx:        context.WithValue(context.Background(), "hourBucket", time.Unix(1444262400, 0).UTC()),
	})
}

func TestUploadWithIsoDateFieldPartitioner(t *testing.T) {
	s3ClientMock := s3.New(session.New())
	s3ClientMock.Handlers.Clear()
	s3ClientMock.Handlers.Send.PushBack(func(r *request.Request) {
		switch x := r.Params.(type) {
		case *s3.PutObjectInput:
			bt, err := ioutil.ReadAll(x.Body)
			require.NoError(t, err)
			require.Equal(t, "{\"message\": 1, \"ts\": \"2015-10-08T00:00:00Z\"}{\"message\": 2, \"ts\": \"2015-10-08T00:00:01Z\"}{\"message\": 3, \"ts\": \"2015-10-08T00:00:02Z\"}", string(bt))
			require.Contains(t, *x.Key, "backup/topic=testTopic2/date=2015-10-08/hour=00/0010.808.")
			require.Equal(t, *x.Bucket, "test")
		}
	})

	// Create new temporary file to upload.
	f, err := ioutil.TempFile("", "kafka-archiver-upload-test")
	require.NoError(t, err)
	f.WriteString(`{"message": 1, "ts": "2015-10-08T00:00:00Z"}`)
	f.WriteString(`{"message": 2, "ts": "2015-10-08T00:00:01Z"}`)
	f.WriteString(`{"message": 3, "ts": "2015-10-08T00:00:02Z"}`)

	require.NoError(t, f.Close())

	hourBucket, err := time.Parse(time.RFC3339Nano, "2015-10-08T00:00:00Z")
	require.NoError(t, err)

	w := newTestWorker(partitioner.New(&partitioner.Config{
		Type:            "IsoDateFieldPartitioner",
		FieldKeyName:    "ts",
		TopicNamePrefix: "topic=",
		BaseFolder:      "backup",
	}), s3ClientMock)

	w.upload(&buffer.Flush{
		Topic:      "testTopic2",
		Path:       f.Name(),
		Partition:  10,
		LastOffset: 808,
		Ctx:        context.WithValue(context.Background(), "hourBucket", hourBucket.UTC()),
	})
}
