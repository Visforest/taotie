package server

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

//  Kafka 消息缓冲区大小
const ChBuf = 1 << 16

var ChMsg = make(chan kafka.Message, ChBuf)

func GenMessage(topic string, data *map[string]interface{}) (*kafka.Message, error) {
	uuidKey, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	bytes, err := json.Marshal(*data)
	if err != nil {
		return nil, err
	}
	return &kafka.Message{
		Topic: topic,
		Key:   []byte(uuidKey.String()),
		Value: bytes,
	}, nil
}

func BufMsg(msgs ...*kafka.Message) {
	if len(ChMsg)+len(msgs) < ChBuf {
		for _, m := range msgs {
			ChMsg <- *m
		}
	}
	//	todo
}
