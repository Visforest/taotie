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
	// finished writing
	finishCache Direction = 4
)

var chMsg = make(chan kafka.Message, bufSize)
var chCache = make(chan kafka.Message, bufSize)
var chDirection = make(chan Direction)

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
	var batchMsgs = make([]kafka.Message, KafkaBatchSize)
	var msgIdx int
	var forwardToCache = false
	for {
	fetchMsg:
		c, cancel := context.WithTimeout(ctx, time.Second)
		// reset msgIdx
		msgIdx = 0
		for {
			select {
			case m := <-chMsg:
				if forwardToCache {
					// abnormal, forward msg to cache buffer
					chCache <- m
				} else {
					// normal
					batchMsgs[msgIdx] = m
					msgIdx++
					if msgIdx == KafkaBatchSize {
						// reached batch threshold, batch write
						if err := KafkaWriter.WriteMessages(ctx, batchMsgs...); err != nil {
							ServerLogger.Errorf(ctx, err, "write msg to kafka failed")
							for _, failMsg := range batchMsgs {
								chCache <- failMsg
							}
						}
						cancel()
						goto fetchMsg
					}
				}
			case <-c.Done():
				// timedout, flush msgs
				if err := KafkaWriter.WriteMessages(ctx, batchMsgs[:msgIdx]...); err != nil {
					ServerLogger.Errorf(ctx, err, "write msg to kafka failed")
					for _, failMsg := range batchMsgs[:msgIdx] {
						chCache <- failMsg
					}
				}
				goto fetchMsg
			case direction := <-chDirection:
				cancel()
				switch direction {
				case rushStage:
					for _, interupptedMsg := range batchMsgs[:msgIdx] {
						chCache <- interupptedMsg
					}
					chDirection <- finishCache
					runtime.Goexit()
					return
				case temporaryStage:
					// kafka is temporarily available, stop writing to kafka, write to local instead
					for _, interupptedMsg := range batchMsgs[:msgIdx] {
						chCache <- interupptedMsg
					}
					forwardToCache = true
					// reset msgIdx
					msgIdx = 0
				case resume:
					// kafka is available again, continue to write to kafka
					forwardToCache = false
				}
			default:
				continue
			}
		}
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
					if err == nil {
						ServerLogger.Debugf(ctx, "%s unlock successed", fileCache.id)
					} else {
						ServerLogger.Errorf(ctx, err, "%s unlock failed", fileCache.id)
					}
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
	for {
		ctx, cancel := context.WithTimeout(ctx, time.Minute)

		var cache *FileCache
		var timedout = false
		var stopWrite = false
		// monitor timeout and direction
		go func() {
			select {
			case <-ctx.Done():
				timedout = true
			case direction := <-chDirection:
				switch direction {
				case finishCache:
					stopWrite = true
					cancel()
				}
			}
		}()
		for {
			if timedout {
				ServerLogger.Debugf(ctx, "cache msg,timedout,break")
				break
			}
			select {
			case m := <-chCache:
				if cache == nil {
					var err error
					cache, err = NewCache()
					ServerLogger.Debugf(ctx, "%s cache created", cache.id)
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
				if stopWrite {
					ServerLogger.Infof(ctx, "start to stop writing cache")
					break
				}
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
		if stopWrite {
			ServerLogger.Infof(ctx, "stoped writing cache")
			break
		}
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
			normal = false
			dur = time.Second * 5
		} else {
			if !normal {
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
