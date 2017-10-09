// +build integration

package stream

import (
	"syscall"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
)

const (
	defaultAWSID     = ""
	defaultAWSSecret = ""
	defaultAWSToken  = ""
	defaultAWSRegion = "eu-west-1"
	defaultAWSStream = ""
)

func TestRemoteStream_Integration(t *testing.T) {
	// Don't run this in parallel

	// rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	config, err := BuildConfig(
		WithRegion(GetEnv("AWS_FIREHOSE_REGION", defaultAWSRegion)),
		WithID(GetEnv("AWS_FIREHOSE_ID", defaultAWSID)),
		WithSecret(GetEnv("AWS_FIREHOSE_SECRET", defaultAWSSecret)),
		WithToken(GetEnv("AWS_FIREHOSE_TOKEN", defaultAWSToken)),
		WithStream(GetEnv("AWS_FIREHOSE_STREAM", defaultAWSStream)),
		WithMaxNumberOfMessages(10),
		WithVisibilityTimeout(time.Second*100),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("new", func(t *testing.T) {
		queue, err := NewRemoteStream(config, log.NewNopLogger())
		if err != nil {
			t.Fatal(err)
		}

		if expected, actual := false, queue == nil; expected != actual {
			t.Errorf("expected: %t, actual: %t", expected, actual)
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
