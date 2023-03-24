package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
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

type FileCache struct {
	sync.Mutex
	id          string
	cacheFile   string
	idxFile     string
	lockFile    string
	readCacheAt int64
	readIdxAt   int64
}

func (c *FileCache) WriteToKafka(ctx context.Context) error {
	idxFile, err := os.Open(c.idxFile)
	if err != nil {
		return err
	}
	defer idxFile.Close()
	cacheFile, err := os.Open(c.cacheFile)
	if err != nil {
		return err
	}
	defer cacheFile.Close()
	lockFile, err := os.Create(c.lockFile)
	if err != nil {
		return err
	}
	lockFile.WriteString(c.id)
	defer lockFile.Close()

	defer func() {
		// truncate data that are already written to kafka
		cacheFile.Truncate(c.readCacheAt)
		idxFile.Truncate(c.readIdxAt)
	}()

	idxReader := bufio.NewReader(idxFile)
	for {
		line, err := idxReader.ReadString('\n')
		if len(line) != 0 {
			msgByteLen, err := strconv.ParseInt(line, 10, 64)
			if err != nil {
				return err
			}
			msgBytes := make([]byte, msgByteLen)
			_, err = cacheFile.Read(msgBytes)
			if err != nil {
				return err
			}
			var msg kafka.Message
			if err := json.Unmarshal(msgBytes, &msg); err != nil {
				return err
			}
			if err := KafkaWriter.WriteMessages(ctx, msg); err != nil {
				ChCache <- msg
				return err
			}
			// update read progress
			c.readCacheAt += msgByteLen
			c.readIdxAt += int64(len(line))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *FileCache) Clear() error {
	c.Lock()
	defer c.Unlock()
	var err error
	err = os.Remove(c.cacheFile)
	if err != nil {
		return err
	}
	err = os.Remove(c.idxFile)
	if err != nil {
		return err
	}
	err = os.Remove(c.lockFile)
	if err != nil {
		return err
	}
	return nil
}

// RewriteMsg rewrite msgs to kafka
func RewriteMsg(ctx context.Context) {
	if err := os.MkdirAll(GlbConfig.Data.DataDir, 0666); err != nil {
		ServerLogger.Panicf(ctx, err, "make data dir: %s failed", GlbConfig.Data.DataDir)
	}
	fileInfos, err := os.ReadDir(GlbConfig.Data.DataDir)
	if err != nil {
		ServerLogger.Panicf(ctx, err, "read data dir:%s failed", GlbConfig.Data.DataDir)
	}
	files := make(map[string]*FileCache)
	reg := regexp.MustCompile(`^cache_(\w+).(cache|idx|lock)$`)
	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}
		tmp := reg.FindStringSubmatch(fileInfo.Name())
		if len(tmp) < 1 {
			ServerLogger.Warnf(ctx, "bad cache file:%s", fileInfo.Name())
			continue
		}
		cacheId := tmp[0]
		cache, ok := files[cacheId]
		if !ok {
			cache = &FileCache{id: cacheId}
		}

		if strings.HasSuffix(cacheId, ".cache") {
			cache.cacheFile = fmt.Sprintf("%s/%s", GlbConfig.Data.DataDir, fileInfo)
		} else if strings.HasSuffix(cacheId, ".idx") {
			cache.idxFile = fmt.Sprintf("%s/%s", GlbConfig.Data.DataDir, fileInfo)
		} else if strings.HasSuffix(cacheId, ".lock") {
			cache.lockFile = fmt.Sprintf("%s/%s", GlbConfig.Data.DataDir, fileInfo)
		}
	}
	for cacheId, cache := range files {
		if cache.lockFile != "" {
			continue
		}
		if cache.idxFile == "" {
			ServerLogger.Warnf(ctx, "%s idx file not found", cacheId)
			continue
		}
		if cache.cacheFile == "" {
			ServerLogger.Warnf(ctx, "%s cache file not found", cacheId)
			continue
		}
		// rewrite to kafka
		go func(fileCache *FileCache) {
			fileCache.Lock()
			defer fileCache.Unlock()
			err := fileCache.WriteToKafka(ctx)
			if err == nil {
				// rewrite all msgs,clear cache
				err = fileCache.Clear()
				if err != nil {
					ServerLogger.Errorf(ctx, err, "clear cache data failed")
				}
			} else {
				// rewrite failed
				ServerLogger.Errorf(ctx, err, "rewrite to kafka failed")
			}
		}(cache)
	}
}

// CacheMsg cache msgs to local files
func CacheMsg(ctx context.Context) {
	if err := os.MkdirAll(GlbConfig.Data.DataDir, 0666); err != nil {
		ServerLogger.Errorf(ctx, err, "make data dir: %s failed", GlbConfig.Data.DataDir)
	}

	for {
		ctx, cancel := context.WithTimeout(ctx, time.Minute)

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

		var timedout = false
		go func() {
			select {
			case <-ctx.Done():
				timedout = true
				cancel()
			}
		}()
		for m := range ChCache {
			if timedout {
				break
			}
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
