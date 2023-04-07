package server

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	KafkaBatchSize = 1 << 5
)

var KafkaWriter *kafka.Writer

var kafkaConn *kafka.Conn

func InitKafka(ctx context.Context) {
	KafkaWriter = &kafka.Writer{
		Addr:                   kafka.TCP(GlbConfig.Kafka.Broker...),
		Balancer:               &kafka.LeastBytes{},
		WriteTimeout:           time.Millisecond * time.Duration(GlbConfig.Kafka.WriteTimeout),
		RequiredAcks:           kafka.RequiredAcks(GlbConfig.Kafka.AckPolicy),
		Async:                  false,
		BatchSize:              KafkaBatchSize,
		Compression:            kafka.Lz4,
		AllowAutoTopicCreation: true,
	}
	var err error
	kafkaConn, err = kafka.Dial("tcp", GlbConfig.Kafka.Broker[0])
	if err != nil {
		ServerLogger.Panicf(ctx, err, "connect to kafka node %s failed", GlbConfig.Kafka.Broker[0])
	}
}
