package server

import (
	"github.com/segmentio/kafka-go"
	"time"
)

var KafkaWriter *kafka.Writer

func InitKafka() {
	KafkaWriter = &kafka.Writer{
		Addr:                   kafka.TCP(GlbConfig.Kafka.Broker...),
		Balancer:               &kafka.LeastBytes{},
		WriteTimeout:           time.Millisecond * time.Duration(GlbConfig.Kafka.WriteTimeout),
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
		BatchSize:              1,
		Compression:            kafka.Lz4,
		AllowAutoTopicCreation: true,
	}
}
