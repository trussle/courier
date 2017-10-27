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
	"github.com/trussle/courier/pkg/audit"
	"github.com/trussle/courier/pkg/consumer"
	h "github.com/trussle/courier/pkg/http"
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/status"
	"github.com/trussle/fsys"
)

const (
	defaultQueue            = "remote"
	defaultAuditLog         = "nop"
	defaultAuditLogRootPath = "bin"
	defaultFilesystem       = "nop"

	defaultAWSID     = ""
	defaultAWSSecret = ""
	defaultAWSToken  = ""
	defaultAWSRegion = "eu-west-1"

	defaultAWSSQSQueue       = ""
	defaultAWSFirehoseStream = ""

	defaultRecipientURL        = ""
	defaultNumConsumers        = 2
	defaultMaxNumberOfMessages = 5
	defaultVisibilityTimeout   = "1s"
	defaultMetricsRegistration = true
)

func runIngest(args []string) error {
	// flags for the ingest command
	var (
		flags = flagset.NewFlagSet("ingest", flag.ExitOnError)

		debug   = flags.Bool("debug", false, "debug logging")
		apiAddr = flags.String("api", defaultAPIAddr, "listen address for ingest API")

		awsID     = flags.String("aws.id", defaultAWSID, "AWS configuration id")
		awsSecret = flags.String("aws.secret", defaultAWSSecret, "AWS configuration secret")
		awsToken  = flags.String("aws.token", defaultAWSToken, "AWS configuration token")
		awsRegion = flags.String("aws.region", defaultAWSRegion, "AWS configuration region")

		awsSQSQueue       = flags.String("aws.sqs.queue", defaultAWSSQSQueue, "AWS configuration queue")
		awsFirehoseStream = flags.String("aws.firehose.stream", defaultAWSFirehoseStream, "AWS configuration stream")

		queueType        = flags.String("queue", defaultQueue, "type of queue to use (remote, virtual, nop)")
		auditLogType     = flags.String("auditlog", defaultAuditLog, "type of audit log to use (remote, local, nop)")
		auditLogRootPath = flags.String("auditlog.path", defaultAuditLogRootPath, "audit log root directory for the filesystem to use")
		filesystemType   = flags.String("filesystem", defaultFilesystem, "type of filesystem backing (local, virtual, nop)")

		recipientURL = flags.String("recipient.url", defaultRecipientURL, "URL to hit with the message payload")
		numConsumers = flags.Int("num.consumers", defaultNumConsumers, "number of consumers to run at once")

		maxNumberOfMessages = flags.Int("max.messages", defaultMaxNumberOfMessages, "max number of messages to dequeue at once")
		visibilityTimeout   = flags.String("visibility.timeout", defaultVisibilityTimeout, "how long the visibility of a message should extended by in seconds")

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
	failedSegments := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "courier_transformer_store",
		Name:      "store_failed_segments",
		Help:      "Segments failed from ingest.",
	})
	failedRecords := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "courier_transformer_store",
		Name:      "store_failed_records",
		Help:      "Records failed from ingest.",
	})

	if *metricsRegistration {
		prometheus.MustRegister(
			connectedClients,
			apiDuration,
			consumedSegments,
			consumedRecords,
			replicatedSegments,
			replicatedRecords,
			failedSegments,
			failedRecords,
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
	auditRemoteConfig, err := audit.BuildRemoteConfig(
		audit.WithID(*awsID),
		audit.WithSecret(*awsSecret),
		audit.WithToken(*awsToken),
		audit.WithRegion(*awsRegion),
		audit.WithStream(*awsFirehoseStream),
	)
	if err != nil {
		return errors.Wrap(err, "audit remote config")
	}

	// Create the HTTP clients we'll use for various purposes.
	timeoutClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			ResponseHeaderTimeout: 5 * time.Second,
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
		queue.WithID(*awsID),
		queue.WithSecret(*awsSecret),
		queue.WithToken(*awsToken),
		queue.WithRegion(*awsRegion),
		queue.WithQueue(*awsSQSQueue),
		queue.WithMaxNumberOfMessages(int64(*maxNumberOfMessages)),
		queue.WithVisibilityTimeout(visibilityTimeoutDuration),
		queue.WithRunFrequency(time.Millisecond*10),
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

	// Execution group.
	g := gexec.NewGroup()
	gexec.Block(g)
	{
		for i := 0; i < *numConsumers; i++ {

			consumerRootDir := filepath.Join(*auditLogRootPath, fmt.Sprintf("audit-%04d", i))
			auditLocalConfig, err := audit.BuildLocalConfig(
				audit.WithRootPath(consumerRootDir),
				audit.WithFsys(fs),
			)
			if err != nil {
				return errors.Wrap(err, "audit local config")
			}

			auditConfig, err := audit.Build(
				audit.With(*auditLogType),
				audit.WithRemoteConfig(auditRemoteConfig),
				audit.WithLocalConfig(auditLocalConfig),
			)
			if err != nil {
				return errors.Wrap(err, "audit config")
			}

			consumerQueue, err := queue.New(queueConfig, log.With(logger, "component", "queue"))
			if err != nil {
				return err
			}

			consumerLog, err := audit.New(auditConfig, log.With(logger, "component", "audit"))
			if err != nil {
				return err
			}

			// Create the consumer
			c := consumer.New(
				h.NewClient(timeoutClient, *recipientURL),
				consumerQueue,
				consumerLog,
				consumedSegments,
				consumedRecords,
				replicatedSegments,
				replicatedRecords,
				failedSegments,
				failedRecords,
				log.With(logger, "component", fmt.Sprintf("consumer-%d", i)),
			)
			g.Add(func() error {
				consumerQueue.Run()
				return nil
			}, func(error) {
				consumerQueue.Stop()
			})
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
				connectedClients.WithLabelValues("ingest"),
				apiDuration,
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
