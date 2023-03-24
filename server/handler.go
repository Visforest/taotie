package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"os"
	"time"
)

// ChBuf channel buffer size
const ChBuf = 1 << 16

var ChMsg = make(chan kafka.Message, ChBuf)
var ChCache = make(chan kafka.Message, ChBuf)

func GenMessage(topic string, data *map[string]interface{}) (*kafka.Message, error) {
	bytes, err := json.Marshal(*data)
	if err != nil {
		return nil, err
	}
	return &kafka.Message{
		Topic: topic,
		Key:   []byte(uuid.NewString()),
		Value: bytes,
	}, nil
}

func BufMsg(msgs ...*kafka.Message) {
	for _, m := range msgs {
		select {
		case ChMsg <- *m:
		default:
			// ChMsg is full，turn to cache msg
			ChCache <- *m
		}
	}
}

// WriteMsg write msgs to kafka
func WriteMsg(ctx context.Context) {
	for m := range ChMsg {
		if err := KafkaWriter.WriteMessages(ctx, m); err != nil {
			ServerLogger.Errorf(ctx, err, "write msg to kafka failed")
			ChCache <- m
		}
	}
}

// RewriteMsg rewrite msgs to kafka
func RewriteMsg(ctx context.Context) {

}

// CacheMsg cache msgs to local files
func CacheMsg(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		cacheId := uuid.NewString()
		// cache file, where msgs are cached
		cachePath := fmt.Sprintf("%s/cache_%s.cache", GlbConfig.Data.DataDir, cacheId)
		// cache idx file, in which each msg size is record line by line
		idxPath := fmt.Sprintf("%s/cache_%s.idx", GlbConfig.Data.DataDir, cacheId)
		// cache file lock, represents whether the cache file with the same cacheId is used
		lockPath := fmt.Sprintf("%s/cache_%s.lock", GlbConfig.Data.DataDir, cacheId)

		cacheFile, err := os.OpenFile(cachePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0660)
		if err != nil {
			ServerLogger.Errorf(ctx, err, "create cache file failed")
			continue
		}

		idxFile, err := os.OpenFile(idxPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0660)
		if err != nil {
			ServerLogger.Errorf(ctx, err, "create idx file failed")
			continue
		}

		lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0660)
		if err != nil {
			ServerLogger.Errorf(ctx, err, "create lock file failed")
			continue
		}

		lockFile.WriteString(cacheId)
		for m := range ChCache {
			if mbytes, err := json.Marshal(m); err == nil {
				if byteLen, err := cacheFile.Write(mbytes); err == nil {
					idxFile.WriteString(fmt.Sprintf("%d\n", byteLen))
				} else {
					ServerLogger.Errorf(ctx, err, "record cache idx failed")
				}
			} else {
				ServerLogger.Errorf(ctx, err, "json encode msg failed:%+v", m)
			}
		}

		if err := cacheFile.Close(); err != nil {
			ServerLogger.Errorf(ctx, err, "close %s failed", cachePath)
		}
		if err := idxFile.Close(); err != nil {
			ServerLogger.Errorf(ctx, err, "close %s failed", idxPath)
		}
		if err := os.Remove(lockPath); err != nil {
			ServerLogger.Errorf(ctx, err, "rm %s failed", lockPath)
		}
	}
}

// Sentinel monitor kafka status and provides advice
func Sentinel() {

}
