package server

import "github.com/segmentio/kafka-go"

//  Kafka 消息缓冲区大小
const ChBuf = 1 << 16

var ChMsg = make(chan kafka.Message, ChBuf)

func BufMsg(msgs ...*kafka.Message) {
	if len(ChMsg)+len(msgs) < ChBuf {
		for _, m := range msgs {
			ChMsg <- *m
		}
	}
	//	todo
}
