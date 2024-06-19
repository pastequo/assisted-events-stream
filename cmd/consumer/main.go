package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/openshift-assisted/assisted-events-streams/internal/projection"
	"github.com/openshift-assisted/assisted-events-streams/internal/utils"
	"github.com/openshift-assisted/assisted-events-streams/pkg/stream"
	kafka "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

const AckChannelBufferSize = 1000

func main() {
	ctx := context.Background()
	log := utils.NewLogger()
	ackChannel := make(chan kafka.Message, AckChannelBufferSize)
	reader, err := stream.NewKafkaReader(log, ackChannel)
	if err != nil {
		log.WithError(err).Fatal("Could not connect to kafka")
	}

	projection := projection.NewEnrichedEventsProjectionFromEnv(ctx, log, ackChannel)

	intChannel := make(chan os.Signal, 1)

	gracefulShutdown := func() {
		for sig := range intChannel {
			log.WithFields(logrus.Fields{
				"signal": sig,
			}).Info("Captured signal, shutting down")
			reader.Close(ctx)
			projection.Close(ctx)
		}
	}

	signal.Notify(intChannel, syscall.SIGTERM, syscall.SIGINT)
	go gracefulShutdown()

	err = reader.Consume(ctx, projection.ProcessMessage)
	if err != nil {
		log.WithError(err).Fatal(err)
	}
}
