package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/Shopify/sarama"
	"github.com/UnityTech/kafka-archiver/buffer"
	"github.com/UnityTech/kafka-archiver/partitioner"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	cluster "github.com/bsm/sarama-cluster"
	humanize "github.com/dustin/go-humanize"
	"github.com/golang/glog"
	"github.com/vrischmann/flagutil"
)

var (
	metrics *statsd.Client
)

// NewWorker simple helper function to allocate a worker that will need to be initialized.
func NewWorker() *Worker {
	return new(Worker)
}

// Worker Contains all of the necessary configuration data to fetch from Kafka and log to S3.
type Worker struct {
	consumer    *cluster.Consumer
	producer    sarama.SyncProducer
	uploader    *s3manager.Uploader
	bufferFlush chan *buffer.Flush
	wg          sync.WaitGroup
	partitioner partitioner.Partitioner

	// Kafka Flags.
	consumerGroup           string
	consumerDelayStart      flagutil.Duration
	consumerAutoOffsetReset string
	brokers                 flagutil.Strings
	topics                  flagutil.Strings
	topicWhitelist          string
	topicBlacklist          string

	// Buffer Flags.
	bufferFlushInterval flagutil.Duration
	bufferFlushSize     string
	bufferMem           string
	bufferLocation      string
	bufferQueueLength   int

	// Partitioner Flags.
	partitionerClass               string
	partitionerFieldName           string
	partitionerPathBaseFolder      string
	partitionerPathTopicNamePrefix string

	// S3 Uploader Flags.
	s3enabled           bool
	s3region            string
	s3bucket            string
	s3endpoint          string
	s3ClientDebug       bool
	s3ForcePathStyle    bool
	s3NotificationTopic string
	s3PartSize          string
	s3Concurrency       int

	// Other Flags.
	datadogEnabled bool
	datadogHost    string
	statsdPrefix   string
}

// Init is designed to be called only once. This function initialized the context of the application,
// performs flags validation, sets up all the necessary components
func (w *Worker) Init() {

	// Define application flags using the `flag` package.
	flag.Var(&w.brokers, "brokers",
		"Kafka brokers to connect to, as a comma separated list. (required)")
	flag.Var(&w.topics, "topics",
		"Kafka topics to subscribe to.")
	flag.StringVar(&w.topicWhitelist, "topic-whitelist", "",
		"An additional whitelist of topics to subscribe to.")
	flag.StringVar(&w.topicBlacklist, "topic-blacklist", "",
		"An additional blacklist of topics, precedes the whitelist.")
	flag.StringVar(&w.consumerGroup, "consumer-group", "",
		"Name of your kafka consumer group. (required)")
	flag.StringVar(&w.consumerAutoOffsetReset, "consumer-auto-offset-reset", "oldest",
		"Kafka consumer group topic default offset.")
	flag.Var(&w.consumerDelayStart, "consumer-delay-start",
		"Number of seconds to wait before starting consuming. (default \"0s\")")

	// Define Buffer flags.
	flag.Var(&w.bufferFlushInterval, "buffer-interval",
		"The duration when the buffer should close the file. (default \"15m\")")
	flag.StringVar(&w.bufferFlushSize, "buffer-size", "100MiB",
		"The size in bytes when the buffer should close the file.")
	flag.IntVar(&w.bufferQueueLength, "buffer-queue-length", 32,
		"The size of the queue to hold the files to be uploaded.")
	flag.StringVar(&w.bufferLocation, "buffer-location", os.TempDir(),
		"The base folder where to store temporary files.")
	flag.StringVar(&w.bufferMem, "buffer-mem", "8KB",
		"Amount of memory a buffer can use.")

	// Define Partitioner flags.
	flag.StringVar(&w.partitionerClass, "partitioner", "DefaultPartitioner",
		"The name of the partitioner to use.")
	flag.StringVar(&w.partitionerFieldName, "partitioner-key", "",
		"Name of the JSON field to parse.")
	flag.StringVar(&w.partitionerPathBaseFolder,
		"partitioner-path-folder", "backup",
		"The top level folder to prepend to the path used when partitioning files.")
	flag.StringVar(&w.partitionerPathTopicNamePrefix,
		"partitioner-path-topic-prefix", "topic=",
		"A prefix to prepend to the path used when partitioning files.")

	// Define S3 flags.
	flag.BoolVar(&w.s3enabled, "s3", false, "Enable S3 uploader.")
	flag.StringVar(&w.s3region, "s3-region", "", "S3 Bucket Region.")
	flag.StringVar(&w.s3bucket, "s3-bucket", "",
		"S3 Bucket where to upload files.")
	flag.StringVar(&w.s3endpoint, "s3-endpoint", "",
		"S3 Bucket Endpoint to use for the client.")
	flag.StringVar(&w.s3PartSize, "s3-part-size", "5MiB",
		"S3 Uploader part size.")
	flag.IntVar(&w.s3Concurrency, "s3-concurrency", 5,
		"S3 Uploader part size.")
	flag.BoolVar(&w.s3ForcePathStyle, "s3-force-path-style", false,
		"Enable to force the request to use path-style addressing on S3.")
	flag.BoolVar(&w.s3ClientDebug, "s3-client-debug", false,
		"Enable to enable debug logging on S3 client.")
	flag.StringVar(&w.s3NotificationTopic, "s3-notification-topic", "",
		"Kafka topic used to store uploaded S3 files.")

	// Define other flags.
	flag.BoolVar(&w.datadogEnabled, "datadog", true, "Enable sending metrics to Statsd/Datadog")
	flag.StringVar(&w.datadogHost, "datadog-host",
		flagutil.EnvOrDefault("DATADOG_HOST", "localhost:2585"),
		"The host where the datadog agents listens to.")
	flag.StringVar(&w.statsdPrefix, "statsd-prefix", "kafka-archiver",
		"The name prefix for statsd metrics.")

	// Parse the flags.
	flag.Parse()

	// Validate the configuration and set default values if necessary.
	if len(w.brokers) == 0 {
		glog.Fatal("no bootstrap brokers provided")
	}

	if len(w.topics) == 0 && w.topicWhitelist == "" {
		glog.Fatal("no topics or topic-whitelist provided")
	}

	if w.consumerGroup == "" {
		glog.Fatal("no consumer group provided")
	}

	if w.bufferFlushInterval.Duration == 0 {
		glog.Info("setting flush interval to default of 15 minutes.")
		if err := w.bufferFlushInterval.Set("15m"); err != nil {
			glog.Fatal("could not parse time interval for flushing")
		}
	}

	if !strings.Contains(w.datadogHost, ":") {
		w.datadogHost = net.JoinHostPort(w.datadogHost, "8125")
	}

	if w.s3enabled {
		if w.s3bucket == "" {
			glog.Fatal("no s3-bucket provided")
		}

		if w.s3region == "" {
			glog.Fatal("no s3-region provided")
		}

		glog.Infof("using S3 bucket %q in region %q", w.s3bucket, w.s3region)
	}

	// Create Kafka producer.
	if w.s3NotificationTopic != "" {
		var err error
		producerConfig := sarama.NewConfig()
		producerConfig.Version = sarama.V0_10_0_1
		producerConfig.Producer.Compression = sarama.CompressionSnappy
		producerConfig.Producer.Retry.Max = 10
		producerConfig.Producer.Return.Successes = true
		producerConfig.Producer.Return.Errors = true
		producerConfig.Producer.RequiredAcks = sarama.WaitForLocal
		producerConfig.Producer.Retry.Backoff = 500 * time.Millisecond

		w.producer, err = sarama.NewSyncProducer(w.brokers, producerConfig)
		if err != nil {
			glog.Fatal(err)
		}
	}

	// Create buffer configuration.
	bufferFlushSize, err := humanize.ParseBytes(w.bufferFlushSize)
	if err != nil {
		glog.Fatal(err)
	}

	bufferMemBytes, err := humanize.ParseBytes(w.bufferMem)
	if err != nil {
		glog.Fatal(err)
	}

	w.bufferFlush = make(chan *buffer.Flush, w.bufferQueueLength)

	bufferConfig := &buffer.Config{
		FlushBytes:    int64(bufferFlushSize),
		FlushInterval: w.bufferFlushInterval.Duration,
		Queue:         w.bufferFlush,
		TempDir:       w.bufferLocation,
		BufferSize:    int(bufferMemBytes),
	}

	glog.Infof(
		"created buffer configuration flush-bytes=%v flush-interval=%v buffer-mem=%d",
		int64(bufferFlushSize),
		w.bufferFlushInterval.Duration,
		int(bufferMemBytes),
	)

	// Create partitioner.
	w.partitioner = partitioner.New(&partitioner.Config{
		Type:                w.partitionerClass,
		BaseFolder:          w.partitionerPathBaseFolder,
		TopicNamePrefix:     w.partitionerPathTopicNamePrefix,
		FieldKeyName:        w.partitionerFieldName,
		DefaultBufferConfig: bufferConfig,
	})

	if w.datadogEnabled {
		// Setup datadog metrics client.
		metrics, err = statsd.New(w.datadogHost)
		if err != nil {
			glog.Fatal(err)
		}
		metrics.Namespace = fmt.Sprintf("%s.", w.statsdPrefix)
		glog.Infof("created datadog client host=%s", w.datadogHost)
	}

	// Setup Cloud Providers.
	if w.s3enabled {

		// Create AWS Configuration.
		awsConfig := &aws.Config{
			Region:           &w.s3region,
			Endpoint:         &w.s3endpoint,
			MaxRetries:       aws.Int(10),
			S3ForcePathStyle: aws.Bool(w.s3ForcePathStyle),
		}

		// Set AWS Session Log Level.
		if w.s3ClientDebug {
			awsConfig.LogLevel = aws.LogLevel(aws.LogDebug)
		}

		sess := session.Must(session.NewSession(awsConfig))

		// Create AWS Uploader.
		w.uploader = s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
			s3PartSize, err := humanize.ParseBytes(w.s3PartSize)
			if err != nil {
				glog.Fatal(err)
			}

			u.PartSize = int64(s3PartSize)
			u.LeavePartsOnError = false
			u.Concurrency = w.s3Concurrency
		})

		glog.Infof("created s3 uploader region=%s bucket=%s", w.s3region, w.s3bucket)
	}

	// Intialize sarama logger if -v >= 5.
	if glog.V(5) {
		glog.Infoln("Sarama logger initialized")
		sarama.Logger = log.New(os.Stdout, "sarama] ", log.LstdFlags)
	}

	// Create sarama cluster configuration.
	config := cluster.NewConfig()
	config.Version = sarama.V0_10_0_1
	config.Consumer.Return.Errors = true
	config.Consumer.Retry.Backoff = 5 * time.Second
	config.Net.DialTimeout = 30 * time.Second
	config.Net.KeepAlive = 60 * time.Second
	config.ClientID = "kafka-archiver"
	config.Group.Return.Notifications = true
	config.Group.PartitionStrategy = cluster.StrategyRoundRobin
	if w.topicWhitelist != "" {
		config.Group.Topics.Whitelist = regexp.MustCompile(w.topicWhitelist)
	}
	if w.topicBlacklist != "" {
		config.Group.Topics.Blacklist = regexp.MustCompile(w.topicBlacklist)
	}

	switch w.consumerAutoOffsetReset {
	case "oldest":
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		glog.Fatal("unknown configuration found for topic-offset, allowed values: oldest,newest")
	}

	// Create KAFKA consumer.
	glog.Infof(
		`creating consumer brokers=%+v topics=%+v 
		topic-whitelist=%+v topic-blacklist=%+v`,
		w.brokers, w.topics, w.topicWhitelist, w.topicBlacklist,
	)

	w.consumer, err = cluster.NewConsumer(w.brokers, w.consumerGroup, w.topics, config)
	if err != nil {
		glog.Fatal(err)
	}

	// Start the buffer listener.
	w.wg.Add(1)
	go w.bufferListener()

	// Start the consumer.
	w.wg.Add(1)
	go w.consume()

	// Wait.
	w.wg.Wait()
}

// bufferListener listens on the channel that declares a file as flushed.
// Calls the upload function to perform a backup to a cloud provider.
// Removes the file and marks the partition offset.
func (w *Worker) bufferListener() {
	defer w.wg.Done()

	for fl := range w.bufferFlush {
		if err := w.upload(fl); err != nil {
			glog.Fatal(err)
		}

		if err := os.Remove(fl.Path); err != nil {
			glog.Error("could not cleanup disk space: ", err)
		}

		w.consumer.MarkPartitionOffset(fl.Topic, fl.Partition, fl.LastOffset, "")
		glog.V(2).Infof("marked offset: topic=%s partition=%d offset=%d",
			fl.Topic, fl.Partition, fl.LastOffset)
	}
}

func (w *Worker) closeBuffers() {
	glog.Info("closing buffer flush channel")

	w.partitioner.Close()
	close(w.bufferFlush)
}
