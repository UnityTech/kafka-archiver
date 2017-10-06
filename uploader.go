package main

import (
	"encoding/json"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/UnityTech/kafka-archiver/buffer"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cenkalti/backoff"
	"github.com/golang/glog"
)

// Notification is a struct that encapsulates a message sent to
// a Kafka topic after a file is flushed.
type Notification struct {
	Provider  string `json:"provider,omitempty"`
	Region    string `json:"region,omitempty"`
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Opened    int64  `json:"opened"`
	Closed    int64  `json:"closed"`
	Age       string `json:"age"`
	Bytes     int64  `json:"bytes"`
	Writes    int64  `json:"writes"`
	Timestamp int64  `json:"timestamp"`
}

// notify is called every time a new file is uploaded, publishes a Notification
// message to the topic specified by the flag `-s3-notification-topic`.
func (w *Worker) notify(fKey string, f *buffer.Flush) error {
	if w.s3NotificationTopic == "" {
		return nil
	}

	// Create a new notification message.
	msg := &Notification{
		Provider:  s3.ServiceName,
		Region:    w.s3region,
		Bucket:    w.s3bucket,
		Key:       fKey,
		Topic:     f.Topic,
		Partition: f.Partition,
		Opened:    f.Opened.Unix(),
		Closed:    f.Closed.Unix(),
		Age:       f.Age.String(),
		Bytes:     f.Bytes,
		Writes:    f.Writes,
		Timestamp: time.Now().Unix(),
	}

	// Marshal the message as JSON.
	val, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Send the message using the Kafka producer.
	_, _, err = w.producer.SendMessage(&sarama.ProducerMessage{
		Topic: w.s3NotificationTopic,
		Key:   sarama.StringEncoder(f.Topic),
		Value: sarama.ByteEncoder(val),
	})

	return err
}

// upload received a flushed file and uses the given cloud provider settings
// to upload the file.
func (w *Worker) upload(f *buffer.Flush) error {
	if !w.s3enabled {
		return nil
	}

	// Get the start time for this function, used for metrics.
	start := time.Now()

	// Open the file.
	fd, err := os.Open(f.Path)
	if err != nil {
		metrics.Incr("error", []string{"kind:s3-upload"}, 1)
		return err
	}
	defer fd.Close()

	// Get the S3 Key used to decide the S3 path/key.
	s3Key := w.partitioner.GetKey(f)

	// Create a new exponential backoff provider.
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 5 * time.Minute
	b.InitialInterval = 5 * time.Second

	// S3 Uploader is wrapped in a backoff function with an exponential provider.
	var s3Response *s3manager.UploadOutput
	backoff.Retry(func() error {
		s3Response, err = w.uploader.Upload(&s3manager.UploadInput{
			Bucket: &w.s3bucket,
			Key:    &s3Key,
			Body:   fd,
		})

		if err != nil {
			metrics.Incr("warn", []string{"kind:s3-upload-retry"}, 1)
			glog.Warningf("upload file failed: %v", err)
		}

		return err
	}, b)

	if err != nil {
		metrics.Incr("error", []string{"kind:s3-upload"}, 1)
		return err
	}

	// Call the notify function with the s3Key and the flushed file.
	if err := w.notify(s3Key, f); err != nil {
		metrics.Incr("error", []string{"kind:notification"}, 1)
		return err
	}

	metrics.Timing("upload_success", time.Since(start), nil, 1)
	glog.V(2).Infof("uploaded file: localpath=%q s3-bucket=%q s3-key=%q request-id=%q", f.Path, w.s3bucket, s3Key, s3Response.UploadID)

	return nil
}
