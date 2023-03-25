package server

import (
	"context"
	"github.com/segmentio/kafka-go"
	"time"
)

var KafkaWriter *kafka.Writer

var kafkaConn kafka.Conn

func InitKafka(ctx context.Context) {
	KafkaWriter = &kafka.Writer{
		Addr:                   kafka.TCP(GlbConfig.Kafka.Broker...),
		Balancer:               &kafka.LeastBytes{},
		WriteTimeout:           time.Millisecond * time.Duration(GlbConfig.Kafka.WriteTimeout),
		RequiredAcks:           kafka.RequiredAcks(GlbConfig.Kafka.AckPolicy),
		Async:                  false,
		BatchSize:              1,
		Compression:            kafka.Lz4,
		AllowAutoTopicCreation: true,
	}
	kafkaConn, err := kafka.Dial("tcp", GlbConfig.Kafka.Broker[0])
	if err != nil {
		ServerLogger.Panicf(ctx, err, "connect to kafka node %s failed", GlbConfig.Kafka.Broker[0])
	}
}
