// +build integration

package audit_test

import (
	"math/rand"
	"syscall"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/trussle/courier/pkg/audit"
	"github.com/trussle/courier/pkg/queue"
	"github.com/trussle/uuid"
)

const (
	defaultAWSID     = ""
	defaultAWSSecret = ""
	defaultAWSToken  = ""
	defaultAWSRegion = "eu-west-1"
	defaultAWSStream = ""
)

func TestRemoteLog_Integration(t *testing.T) {
	// Don't run this in parallel

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	remoteConfig, err := audit.BuildRemoteConfig(
		audit.WithEC2Role(false),
		audit.WithRegion(GetEnv("AWS_REGION", defaultAWSRegion)),
		audit.WithID(GetEnv("AWS_ID", defaultAWSID)),
		audit.WithSecret(GetEnv("AWS_SECRET", defaultAWSSecret)),
		audit.WithToken(GetEnv("AWS_TOKEN", defaultAWSToken)),
		audit.WithStream(GetEnv("AWS_FIREHOSE_STREAM", defaultAWSStream)),
	)
	if err != nil {
		t.Fatal(err)
	}

	config, err := audit.Build(
		audit.With("remote"),
		audit.WithRemoteConfig(remoteConfig),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("new", func(t *testing.T) {
		log, err := audit.New(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := false, log == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
		}
	})

	t.Run("append", func(t *testing.T) {
		log, err := audit.New(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		id, err := uuid.NewWithRand(rnd)
		if err != nil {
			t.Fatal(err)
		}

		record, err := queue.GenerateQueueRecord(rnd)
		if err != nil {
			t.Fatal(err)
		}

		txn := queue.NewTransaction()
		txn.Push(id, record)

		if err := log.Append(txn); err != nil {
			t.Fatal(err)
		}
	})
}

func GetEnv(key string, defaultValue string) (value string) {
	var ok bool
	if value, ok = syscall.Getenv(key); ok {
		return
	}
	return defaultValue
}
