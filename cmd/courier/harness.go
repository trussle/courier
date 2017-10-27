package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/SimonRichardson/flagset"
	"github.com/SimonRichardson/gexec"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/trussle/courier/pkg/harness"
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/courier/pkg/status"
)

const (
	defaultBroadcast = true
)

func runHarness(args []string) error {
	// flags for the harness command
	var (
		flags = flagset.NewFlagSet("ingest", flag.ExitOnError)

		debug   = flags.Bool("debug", false, "debug logging")
		apiAddr = flags.String("api", defaultAPIAddr, "listen address for harness API")

		awsID       = flags.String("aws.id", defaultAWSID, "AWS configuration id")
		awsSecret   = flags.String("aws.secret", defaultAWSSecret, "AWS configuration secret")
		awsToken    = flags.String("aws.token", defaultAWSToken, "AWS configuration token")
		awsRegion   = flags.String("aws.region", defaultAWSRegion, "AWS configuration region")
		awsSQSQueue = flags.String("aws.sqs.queue", defaultAWSSQSQueue, "AWS configuration queue")

		broadcast = flags.Bool("broadcast", defaultBroadcast, "broadcast new records")

		metricsRegistration = flags.Bool("metrics.registration", defaultMetricsRegistration, "Registration of metrics on launch")
	)

	flags.Usage = usageFor(flags, "harness [flags]")
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
	if *metricsRegistration {
		prometheus.MustRegister(
			connectedClients,
			apiDuration,
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

	// Configuration for the queue
	remoteConfig, err := queue.BuildConfig(
		queue.WithID(*awsID),
		queue.WithSecret(*awsSecret),
		queue.WithToken(*awsToken),
		queue.WithRegion(*awsRegion),
		queue.WithQueue(*awsSQSQueue),
	)
	if err != nil {
		return errors.Wrap(err, "queue remote config")
	}

	queueConfig, err := queue.Build(
		queue.With(defaultQueue),
		queue.WithConfig(remoteConfig),
	)
	if err != nil {
		return errors.Wrap(err, "queue config")
	}

	g := gexec.NewGroup()
	gexec.Block(g)
	{
		q, err := queue.New(queueConfig, log.With(logger, "component", "queue"))
		if err != nil {
			return err
		}

		var (
			step = time.NewTicker(500 * time.Millisecond)
			stop = make(chan chan struct{})
		)

		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

		g.Add(func() error {
			for {
				select {
				case <-step.C:
					if !*broadcast {
						continue
					}

					level.Info(logger).Log("state", "enqueuing")

					rec, err := queue.GenerateQueueRecord(rnd)
					if err != nil {
						continue
					}
					if err := q.Enqueue(rec); err != nil {
						level.Error(logger).Log("state", "enqueue failure", "err", err)
						return err
					}

				case q := <-stop:
					level.Info(logger).Log("state", "shutting down...")
					close(q)
					return nil
				}
			}

		}, func(error) {
			q := make(chan struct{})
			stop <- q
			<-q
			return
		})
	}
	{
		g.Add(func() error {
			mux := http.NewServeMux()
			mux.Handle("/", harness.NewAPI(
				log.With(logger, "component", "harness_api"),
			))
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
