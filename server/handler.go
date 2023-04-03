package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

const (
	// bufSize channel buffer size
	bufSize = 1 << 12
	// maxCacheSize max cache file size
	maxCacheSize = 1 << 24
)

type Direction int8

const (
	// rushStage rush to stage data before dying
	rushStage Direction = 1
	// temporaryStage temporarily stage data and may resume in the future
	temporaryStage Direction = 2
	// resume resume to work
	resume Direction = 3
)

var chMsg = make(chan kafka.Message, bufSize)
var chCache = make(chan kafka.Message, bufSize)
var chDirection = make(chan Direction, 2)

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
		case chMsg <- *m:
		default:
			// chMsg is full，turn to cache msg
			chCache <- *m
		}
	}
}

// SendMsg sends msgs to kafka
func SendMsg(ctx context.Context) {

writeKafka:
	for {
		select {
		case m := <-chMsg:
			if err := KafkaWriter.WriteMessages(ctx, m); err != nil {
				ServerLogger.Errorf(ctx, err, "write msg to kafka failed")
				chCache <- m
			}
		case direction := <-chDirection:
			switch direction {
			case rushStage:
				runtime.Goexit()
				return
			case temporaryStage:
				// stop to write to kafka, write to local instead
				goto waitResume
			}
		default:
			continue
		}
	}

waitResume:
	direction := <-chDirection
	if direction == resume {
		goto writeKafka
	}
}

// ResendMsg resends cached msgs to kafka
func ResendMsg(ctx context.Context) {
	ServerLogger.Debugf(ctx, "started to resend msg")
	if err := os.MkdirAll(GlbConfig.Data.DataDir, 0666); err != nil {
		ServerLogger.Panicf(ctx, err, "make data dir: %s failed", GlbConfig.Data.DataDir)
	}
	for {
		fileInfos, err := os.ReadDir(GlbConfig.Data.DataDir)
		if err != nil {
			ServerLogger.Panicf(ctx, err, "read data dir:%s failed", GlbConfig.Data.DataDir)
		}
		files := make(map[string]*FileCache)
		reg := regexp.MustCompile(`cache_(?P<cacheId>[-a-z0-9]+)\.(?:cache|idx|lock)`)
		for _, fileInfo := range fileInfos {
			ServerLogger.Debugf(ctx, "check dir,got %s", fileInfo.Name())
			if fileInfo.IsDir() {
				ServerLogger.Warnf(ctx, "%s is dir,skip", fileInfo.Name())
				continue
			}
			tmp := reg.FindStringSubmatch(fileInfo.Name())
			if len(tmp) < 2 {
				ServerLogger.Warnf(ctx, "bad cache file:%s,reg result:%v", fileInfo.Name(), tmp)
				continue
			}
			cacheId := tmp[1]
			cache, ok := files[cacheId]
			if !ok {
				cache = &FileCache{id: cacheId}
			}

			if strings.HasSuffix(fileInfo.Name(), ".cache") {
				cache.cacheFilePath = fmt.Sprintf("%s/%s", GlbConfig.Data.DataDir, fileInfo.Name())
				cache.cacheFile, _ = os.OpenFile(cache.cacheFilePath, os.O_RDWR, 0)
			} else if strings.HasSuffix(fileInfo.Name(), ".idx") {
				cache.idxFilePath = fmt.Sprintf("%s/%s", GlbConfig.Data.DataDir, fileInfo.Name())
				cache.idxFile, _ = os.OpenFile(cache.idxFilePath, os.O_RDWR, 0)
			} else if strings.HasSuffix(fileInfo.Name(), ".lock") {
				cache.lockFilePath = fmt.Sprintf("%s/%s", GlbConfig.Data.DataDir, fileInfo.Name())
				cache.lockFile, _ = os.OpenFile(cache.lockFilePath, os.O_RDWR, 0)
			}
			files[cacheId] = cache
		}
		for cacheId, cache := range files {
			ServerLogger.Debugf(ctx, "%s resend,read %+v", cacheId, cache)
			if cache.lockFilePath != "" {
				ServerLogger.Warnf(ctx, "%s lock file found,skip", cacheId)
				continue
			}
			if cache.idxFilePath == "" {
				ServerLogger.Warnf(ctx, "%s idx file not found", cacheId)
				continue
			}
			if cache.cacheFilePath == "" {
				ServerLogger.Warnf(ctx, "%s cache file not found", cacheId)
				continue
			}
			// rewrite to kafka
			go func(fileCache *FileCache) {
				fileCache.Lock("resend msgs")
				ServerLogger.Debugf(ctx, "%s locked", fileCache.id)
				defer func() {
					err = fileCache.Unlock()
					if err != nil {
						ServerLogger.Errorf(ctx, err, "%s unlocked failed", fileCache.id)
					}
					ServerLogger.Debugf(ctx, "%s unlocked", fileCache.id)
				}()
				ServerLogger.Debugf(ctx, "%s start to resend", fileCache.id)
				err := fileCache.WriteToKafka(ctx)
				if err == nil {
					// rewrite all msgs,clear cache
					err = fileCache.Clear()
					if err == nil {
						ServerLogger.Debugf(ctx, "%s clear succeeded", fileCache.id)
					} else {
						ServerLogger.Errorf(ctx, err, "%s clear failed", fileCache.id)
					}
				} else {
					// rewrite failed
					ServerLogger.Errorf(ctx, err, "%s resend failed", fileCache.id)
				}
			}(cache)
		}
		time.Sleep(time.Second * 10)
	}
}

// CacheMsg cache msgs to local files
func CacheMsg(ctx context.Context) {
	if err := os.MkdirAll(GlbConfig.Data.DataDir, 0666); err != nil {
		ServerLogger.Errorf(ctx, err, "make data dir: %s failed", GlbConfig.Data.DataDir)
	}

cacheMsg:
	for {
		ctx, cancel := context.WithTimeout(ctx, time.Minute)

		var cache *FileCache
		var timedout = false
		var cancelWrite = false
		var pauseWrite = false
		// monitor timeout and direction
		go func() {
			select {
			case <-ctx.Done():
				timedout = true
			case direction := <-chDirection:
				switch direction {
				case rushStage:
					cancelWrite = true
					cancel()
				case temporaryStage:
					pauseWrite = true
					cancel()
				}
			}
		}()
		for {
			if timedout || cancelWrite || pauseWrite {
				break
			}
			select {
			case m := <-chCache:
				if cache == nil {
					var err error
					cache, err = NewCache()
					if err != nil {
						ServerLogger.Errorf(ctx, err, "create cache failed")
						break
					}
					cache.Lock("cache msgs")
				}
				if mbytes, err := json.Marshal(m); err == nil {
					if cache.wroteAt+len(mbytes) > maxCacheSize {
						break
					}
					cache.WriteCache(mbytes)
				} else {
					ServerLogger.Errorf(ctx, err, "json encode msg failed:%+v", m)
				}
			default:
				continue
			}
		}
		if cache != nil {
			ServerLogger.Debugf(ctx, "%s flush cache", cache.id)
			if err := cache.FlushCache(maxCacheSize); err != nil {
				ServerLogger.Errorf(ctx, err, "%s flush cache failed", cache.id)
			}
			cache.Unlock()
		}
		if cancelWrite {
			break
		}
		if pauseWrite {
			goto waitResume
		}
	}
waitResume:
	direction := <-chDirection
	if direction == resume {
		goto cacheMsg
	}
}

// Sentinel monitor kafka status and provides advice
func Sentinel(ctx context.Context) {
	var normal = true
	dur := time.Second * 10
	for {
		if _, err := kafkaConn.ReadPartitions(); err != nil {
			ServerLogger.Errorf(ctx, err, "detected exception regularly")
			chDirection <- temporaryStage
			chDirection <- temporaryStage
			normal = false
			dur = time.Second * 5
		} else {
			if !normal {
				chDirection <- resume
				chDirection <- resume
				dur = time.Second * 10
			}
		}
		time.Sleep(dur)
	}
}

func saveMsgToLocal(ctx context.Context, ch <-chan kafka.Message) {
	ServerLogger.Debugf(ctx, "saveMsgToLocal")
	cache, err := NewCache()
	if err != nil {
		ServerLogger.Errorf(ctx, err, "create cache file failed")
		return
	}

	cache.Lock("save msg to local")
	defer cache.Unlock()

	for m := range ch {
		if mbytes, err := json.Marshal(m); err == nil {
			cache.WriteCache(mbytes)
		} else {
			ServerLogger.Errorf(ctx, err, "json encode msg failed:%+v", m)
		}
	}
	err = cache.FlushCache(maxCacheSize)
	if err != nil {
		ServerLogger.Errorf(ctx, err, "flush cache failed")
	}
}

// RushStageData rush to save data to local file before dying
func RushStageData(ctx context.Context) {
	ServerLogger.Infof(ctx, "server is stating data...")
	if len(chMsg) == 0 && len(chCache) == 0 {
		// no msgs in channel
		return
	}
	// msgs in channel aren't read completely,turn to save to local
	// and we sent 2, because we have 2 goroutine consumers. A broadcast is better, may be done in the future.
	chDirection <- rushStage
	chDirection <- rushStage
	if len(chMsg) > 0 {
		saveMsgToLocal(ctx, chMsg)
	}
	if len(chCache) > 0 {
		saveMsgToLocal(ctx, chCache)
	}
	close(chDirection)
	ServerLogger.Infof(ctx, "server finished stating data.")
}
