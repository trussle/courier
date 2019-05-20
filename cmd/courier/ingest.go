package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/SimonRichardson/flagset"
	"github.com/SimonRichardson/gexec"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/trussle/courier/pkg/consumer"
	h "github.com/trussle/courier/pkg/http"
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/status"
	"github.com/trussle/courier/pkg/stream"
	"github.com/trussle/fsys"
)

const (
	defaultQueue      = "remote"
	defaultStream     = "virtual"
	defaultFilesystem = "nop"

	defaultRootDir = "bin"

	defaultEC2Role   = true
	defaultAWSID     = ""
	defaultAWSSecret = ""
	defaultAWSToken  = ""
	defaultAWSRegion = "eu-west-1"

	defaultAWSSQSQueue       = ""
	defaultAWSFirehoseStream = ""

	defaultRecipientURL        = ""
	defaultSegmentConsumers    = 2
	defaultMaxNumberOfMessages = 5
	defaultVisibilityTimeout   = "1s"
	defaultTargetBatchSize     = 10
	defaultTargetBatchAge      = "30s"
	defaultMetricsRegistration = true
)

func runIngest(args []string) error {
	// flags for the ingest command
	var (
		flags = flagset.NewFlagSet("ingest", flag.ExitOnError)

		debug   = flags.Bool("debug", false, "debug logging")
		apiAddr = flags.String("api", defaultAPIAddr, "listen address for ingest API")

		awsEC2Role = flags.Bool("aws.ec2.role", defaultEC2Role, "AWS configuration to use EC2 roles")
		awsID      = flags.String("aws.id", defaultAWSID, "AWS configuration id")
		awsSecret  = flags.String("aws.secret", defaultAWSSecret, "AWS configuration secret")
		awsToken   = flags.String("aws.token", defaultAWSToken, "AWS configuration token")
		awsRegion  = flags.String("aws.region", defaultAWSRegion, "AWS configuration region")

		awsSQSQueue       = flags.String("aws.sqs.queue", defaultAWSSQSQueue, "AWS configuration queue")
		awsFirehoseStream = flags.String("aws.firehose.stream", defaultAWSFirehoseStream, "AWS configuration stream")

		queueType      = flags.String("queue", defaultQueue, "type of queue to use (remote, virtual, nop)")
		streamType     = flags.String("stream", defaultStream, "type of stream to use (local, virtual)")
		filesystemType = flags.String("filesystem", defaultFilesystem, "type of filesystem backing (local, virtual, nop)")

		recipientURL     = flags.String("recipient.url", defaultRecipientURL, "URL to hit with the message payload")
		segmentConsumers = flags.Int("segment.consumers", defaultSegmentConsumers, "amount of segment consumers to run at once")

		rootDir = flags.String("root.dir", defaultRootDir, "root directly for the filesystem to use")

		maxNumberOfMessages = flags.Int("max.messages", defaultMaxNumberOfMessages, "max number of messages to dequeue at once")
		visibilityTimeout   = flags.String("visibility.timeout", defaultVisibilityTimeout, "how long the visibility of a message should extended by in seconds")
		targetBatchSize     = flags.Int("target.batch.size", defaultTargetBatchSize, "target batch size before forwarding")
		targetBatchAge      = flags.String("target.batch.age", defaultTargetBatchAge, "target batch age before forwarding")

		metricsRegistration = flags.Bool("metrics.registration", defaultMetricsRegistration, "Registration of metrics on launch")
	)

	flags.Usage = usageFor(flags, "ingest [flags]")
	if err := flags.Parse(args); err != nil {
		return nil
	}

	// Setup the logger.
	var logger log.Logger
	{
		logLevel := level.AllowInfo()
		if *debug {
			logLevel = level.AllowAll()
		}
		logger = log.NewLogfmtLogger(os.Stdout)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC)
		logger = level.NewFilter(logger, logLevel)
	}

	level.Debug(logger).Log("ec2_role", *awsEC2Role, "aws_region", *awsRegion, "aws_sqs_queue", *awsSQSQueue, "aws_firehose_stream", *awsFirehoseStream)

	// Instrumentation
	connectedClients := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "courier_transformer_store",
		Name:      "connected_clients",
		Help:      "Number of currently connected clients by modality.",
	}, []string{"modality"})
	apiDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "courier_transformer_store",
		Name:      "api_request_duration_seconds",
		Help:      "API request duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "path", "status_code"})
	consumedSegments := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "courier_transformer_store",
		Name:      "store_consumed_segments",
		Help:      "Segments consumed from ingest.",
	})
	consumedRecords := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "courier_transformer_store",
		Name:      "store_consumed_records",
		Help:      "Records consumed from ingest.",
	})
	replicatedSegments := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "courier_transformer_store",
		Name:      "store_replicated_segments",
		Help:      "Segments replicated from ingest.",
	})
	replicatedRecords := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "courier_transformer_store",
		Name:      "store_replicated_records",
		Help:      "Records replicated from ingest.",
	})

	if *metricsRegistration {
		prometheus.MustRegister(
			connectedClients,
			apiDuration,
			consumedSegments,
			consumedRecords,
			replicatedSegments,
			replicatedRecords,
		)
	}

	apiNetwork, apiAddress, err := parseAddr(*apiAddr, defaultAPIPort)
	if err != nil {
		return err
	}
	apiListener, err := net.Listen(apiNetwork, apiAddress)
	if err != nil {
		return err
	}
	level.Debug(logger).Log("API", fmt.Sprintf("%s://%s", apiNetwork, apiAddress))

	// Timeout duration setup.
	visibilityTimeoutDuration, err := time.ParseDuration(*visibilityTimeout)
	if err != nil {
		return err
	}

	// Filesystem setup.
	fysConfig, err := fsys.Build(
		fsys.With(*filesystemType),
	)
	if err != nil {
		return errors.Wrap(err, "filesystem config")
	}

	fs, err := fsys.New(fysConfig)
	if err != nil {
		return errors.Wrap(err, "filesystem")
	}

	// Firehose setup.
	streamRemoteConfig, err := stream.BuildConfig(
		stream.WithEC2Role(*awsEC2Role),
		stream.WithID(*awsID),
		stream.WithSecret(*awsSecret),
		stream.WithToken(*awsToken),
		stream.WithRegion(*awsRegion),
		stream.WithStream(*awsFirehoseStream),
		stream.WithMaxNumberOfMessages(int(*maxNumberOfMessages)),
		stream.WithVisibilityTimeout(visibilityTimeoutDuration),
	)
	if err != nil {
		return errors.Wrap(err, "queue remote config")
	}

	// Create the HTTP clients we'll use for various purposes.
	timeoutClient := &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			ResponseHeaderTimeout: 60 * time.Second,
			Dial: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 10 * time.Second,
			DisableKeepAlives:   false,
			MaxIdleConnsPerHost: 1,
		},
	}

	// Configuration for the queue
	queueRemoteConfig, err := queue.BuildConfig(
		queue.WithEC2Role(*awsEC2Role),
		queue.WithID(*awsID),
		queue.WithSecret(*awsSecret),
		queue.WithToken(*awsToken),
		queue.WithRegion(*awsRegion),
		queue.WithQueue(*awsSQSQueue),
		queue.WithMaxNumberOfMessages(int64(*maxNumberOfMessages)),
		queue.WithVisibilityTimeout(visibilityTimeoutDuration),
	)
	if err != nil {
		return errors.Wrap(err, "queue remote config")
	}

	queueConfig, err := queue.Build(
		queue.With(*queueType),
		queue.WithConfig(queueRemoteConfig),
	)
	if err != nil {
		return errors.Wrap(err, "queue config")
	}

	// Configuration for the stream
	age, err := time.ParseDuration(*targetBatchAge)
	if err != nil {
		return err
	}

	// Execution group.
	var g gexec.Group
	gexec.Block(g)
	{
		for i := 0; i < *segmentConsumers; i++ {

			consumerRootDir := filepath.Join(*rootDir, fmt.Sprintf("segment-%04d", i))
			streamConfig, err := stream.Build(
				stream.With(*streamType),
				stream.WithConfig(streamRemoteConfig),
				stream.WithFilesystem(fs),
				stream.WithRootDir(consumerRootDir),
				stream.WithTargetSize(*targetBatchSize),
				stream.WithTargetAge(age),
			)
			if err != nil {
				return errors.Wrap(err, "stream config")
			}

			q, err := queue.New(queueConfig, log.With(logger, "component", "queue"))
			if err != nil {
				return err
			}

			s, err := stream.New(streamConfig, log.With(logger, "component", "stream"))
			if err != nil {
				return err
			}

			// Create the consumer
			c := consumer.New(
				h.NewClient(timeoutClient, *recipientURL),
				q,
				s,
				consumedSegments,
				consumedRecords,
				replicatedSegments,
				replicatedRecords,
				log.With(logger, "component", "consumer"),
			)
			g.Add(func() error {
				c.Run()
				return nil
			}, func(error) {
				c.Stop()
			})
		}
	}
	{
		g.Add(func() error {
			mux := http.NewServeMux()
			mux.Handle("/status/", http.StripPrefix("/status", status.NewAPI(
				log.With(logger, "component", "status_api"),
			)))

			registerMetrics(mux)
			registerProfile(mux)

			return http.Serve(apiListener, mux)
		}, func(error) {
			apiListener.Close()
		})
	}
	gexec.Interrupt(g)
	return g.Run()
}
