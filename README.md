# Kafka Archiver

![](https://github.com/ashleymcnamara/gophers/blob/master/MovingGopher.png)

## Features
- Uses Kafka Client Sarama with Cluster extension
- Stores offsets in Kafka consumer groups (0.9+)
- Includes Datadog (statsd) monitoring
- Uses parallel GZIP implementation
- Multiple partitioners:
  - DefaultPartitioner (topic, partition) 
  - TimeFieldPartitioner (JSON field, Unix Timestamp in milliseconds)
  - IsoDateFieldPartitioner (JSON field, RFC3339Nano)
- Uses S3 Multipart uploader
- Graceful shutdown (with timeout)

## Usage
```
Usage of kafka-archiver:
  -alsologtostderr
      log to standard error as well as files
  -brokers value
      Kafka brokers to connect to, as a comma separated list. (required)
  -buffer-interval value
      The duration when the buffer should close the file. (default "15m")
  -buffer-location string
      The base folder where to store temporary files. (default "$TMPDIR")
  -buffer-mem string
      Amount of memory a buffer can use. (default "8KB")
  -buffer-queue-length int
      The size of the queue to hold the files to be uploaded. (default 32)
  -buffer-size string
      The size in bytes when the buffer should close the file. (default "100MiB")
  -consumer-auto-offset-reset string
      Kafka consumer group topic default offset. (default "oldest")
  -consumer-delay-start value
      Number of seconds to wait before starting consuming. (default "0s")
  -consumer-group string
      Name of your kafka consumer group. (required)
  -datadog-host string
      The host where the datadog agents listens to. (default "localhost:2585")
  -log_backtrace_at value
      when logging hits line file:N, emit a stack trace
  -log_dir string
      If non-empty, write log files in this directory
  -logtostderr
      log to standard error instead of files
  -partitioner string
      The name of the partitioner to use. (default "DefaultPartitioner")
  -partitioner-key string
      Name of the JSON field to parse.
  -partitioner-path-folder string
      The top level folder to prepend to the path used when partitioning files. (default "backup")
  -partitioner-path-topic-prefix string
      A prefix to prepend to the path used when partitioning files. (default "topic=")
  -s3
      Enable S3 uploader.
  -s3-bucket string
      S3 Bucket where to upload files.
  -s3-client-debug
      Enable to enable debug logging on S3 client.
  -s3-concurrency int
      S3 Uploader part size. (default 5)
  -s3-endpoint string
      S3 Bucket Endpoint to use for the client.
  -s3-force-path-style
      Enable to force the request to use path-style addressing on S3.
  -s3-notification-topic string
      Kafka topic used to store uploaded S3 files.
  -s3-part-size string
      S3 Uploader part size. (default "5MiB")
  -s3-region string
      S3 Bucket Region.
  -statsd-prefix string
      The name prefix for statsd metrics. (default "kafka-archiver")
  -stderrthreshold value
      logs at or above this threshold go to stderr
  -topic-blacklist string
      An additional blacklist of topics, precedes the whitelist.
  -topic-whitelist string
      An additional whitelist of topics to subscribe to.
  -topics value
      Kafka topics to subscribe to.
  -v value
      log level for V logs
  -vmodule value
      comma-separated list of pattern=N settings for file-filtered logging
```


## Topic Notification

Kafka Archiver can write a message to a topic upon successful upload. Set the `-s3-notification-topic` to an existing Kafka topic.

The payload of a message looks like the example below.

```json
{
  "provider": "s3",
  "region": "us-west-1",
  "bucket": "test",
  "key": "backup/topic=events/year=2017/month=03/day=24/hour=23/0042.176939634.22dd3d64-2761-4be6-be91-0e70f252dec8.gz",
  "topic": "events",
  "partition": 42,
  "opened": 1490398341,
  "closed": 1490398348,
  "age": "6.532102267s",
  "bytes": 1309890,
  "writes": 2486,
  "timestamp": 1490398350
}
```
