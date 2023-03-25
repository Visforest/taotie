package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/visforest/vftt/utils"
	"io"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	// BufSize channel buffer size
	BufSize = 1 << 12
	// MaxCacheSize max cache file size
	MaxCacheSize = 1 << 16
)

type Direction int8

const (
	// RushStage rush to stage data before dying
	RushStage Direction = 1
	// TemporaryStage temporarily stage data and may resume in the future
	TemporaryStage Direction = 2
	// Resume resume to work
	Resume Direction = 3
)

var ChMsg = make(chan kafka.Message, BufSize)
var ChCache = make(chan kafka.Message, BufSize)
var ChDirection = make(chan Direction)

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

writeKafka:
	for m := range ChMsg {
		select {
		case direction := <-ChDirection:
			switch direction {
			case RushStage:
				runtime.Goexit()
				return
			case TemporaryStage:
				// stop to write to kafka, write to local instead
				return
			}
		default:
			if err := KafkaWriter.WriteMessages(ctx, m); err != nil {
				ServerLogger.Errorf(ctx, err, "write msg to kafka failed")
				ChCache <- m
			}
		}
	}
	direction := <-ChDirection
	if direction == Resume {
		goto writeKafka
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
	idxFile, err := os.OpenFile(c.idxFile, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer idxFile.Close()
	cacheFile, err := os.OpenFile(c.cacheFile, os.O_RDWR, 0)
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
		err := utils.TruncateFile(cacheFile, c.readCacheAt)
		if err != nil {
			ServerLogger.Errorf(ctx, err, "truncate cache file failed")
		}
		err = utils.TruncateFile(idxFile, c.readIdxAt)
		if err != nil {
			ServerLogger.Errorf(ctx, err, "truncate idx file failed")
		}
	}()

	cacheFileInfo, _ := cacheFile.Stat()
	cacheBytes, err := syscall.Mmap(int(cacheFile.Fd()), 0, int(cacheFileInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	idxReader := bufio.NewReader(idxFile)
	for {
		line, err := idxReader.ReadString('\n')
		if len(line) > 1 {
			msgByteLen, err := strconv.ParseInt(strings.TrimRight(line, "\n"), 10, 64)
			if err != nil {
				return err
			}
			msgBytes := cacheBytes[c.readCacheAt : c.readCacheAt+msgByteLen]
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

cacheMsg:
	for {
		ctx, cancel := context.WithTimeout(ctx, time.Minute)

		cacheId := uuid.NewString()
		// cache file, where msgs are cached
		cachePath := fmt.Sprintf("%s/cache_%s.cache", GlbConfig.Data.DataDir, cacheId)
		// cache idx file, in which each msg size is record line by line
		idxPath := fmt.Sprintf("%s/cache_%s.idx", GlbConfig.Data.DataDir, cacheId)
		// cache file lock, represents whether the cache file with the same cacheId is used
		lockPath := fmt.Sprintf("%s/cache_%s.lock", GlbConfig.Data.DataDir, cacheId)

		// cache file, with json encoded kafka message
		cacheFile, err := os.OpenFile(cachePath, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			ServerLogger.Errorf(ctx, err, "create cache file failed")
			continue
		}
		// write placement data
		_, err = cacheFile.Write(make([]byte, MaxCacheSize))
		if err != nil {
			ServerLogger.Errorf(ctx, err, "read&write cache file replacement failed")
			continue
		}
		cacheFileBytes, err := syscall.Mmap(int(cacheFile.Fd()), 0, MaxCacheSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			ServerLogger.Errorf(ctx, err, "read&write cache file failed")
			continue
		}
		// cache index file,with lengths of every json encoded kafka message
		idxFile, err := os.OpenFile(idxPath, os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			ServerLogger.Errorf(ctx, err, "create idx file failed")
			continue
		}
		// cache lock file,indicates whether the cache file is on use and prevents it from being currently modified
		lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			ServerLogger.Errorf(ctx, err, "create lock file failed")
			continue
		}

		lockFile.WriteString(cacheId)

		var timedout = false
		var cancelWrite = false
		var pauseWrite = false
		// monitor timeout and direction
		go func() {
			select {
			case <-ctx.Done():
				timedout = true
			case direction := <-ChDirection:
				switch direction {
				case RushStage:
					cancelWrite = true
					cancel()
				case TemporaryStage:
					pauseWrite = true
					cancel()
				}
			}
		}()
		wroteBytes := 0
		for m := range ChCache {
			if timedout || cancelWrite || pauseWrite {
				break
			}
			if mbytes, err := json.Marshal(m); err == nil {
				if wroteBytes+len(mbytes) > MaxCacheSize {
					break
				}
				copy(cacheFileBytes[wroteBytes:wroteBytes+len(mbytes)], mbytes)
				// record json encoded kafka message length
				idxFile.WriteString(fmt.Sprintf("%d\n", len(mbytes)))
				wroteBytes += len(mbytes)
			} else {
				ServerLogger.Errorf(ctx, err, "json encode msg failed:%+v", m)
			}
		}
		if err = syscall.Munmap(cacheFileBytes); err != nil {
			ServerLogger.Errorf(ctx, err, "unmap %s failed", cachePath)
		}
		if wroteBytes < MaxCacheSize {
			if err = cacheFile.Truncate(int64(MaxCacheSize - wroteBytes)); err != nil {
				ServerLogger.Errorf(ctx, err, "truncate %s failed", cachePath)
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
		if cancelWrite {
			break
		}
		if pauseWrite {
			goto waitResume
		}
	}
waitResume:
	direction := <-ChDirection
	if direction == Resume {
		goto cacheMsg
	}
}

// Sentinel monitor kafka status and provides advice
func Sentinel(ctx context.Context) {
	var normal = true
	for {
		if _, err := kafkaConn.ReadPartitions(); err != nil {
			ServerLogger.Errorf(ctx, err, "detected exception regularly")
			ChDirection <- TemporaryStage
			normal = false
		} else {
			if !normal {
				ChDirection <- Resume
			}
		}
		time.Sleep(time.Second * 5)
	}
}

func saveMsgToLocal(ctx context.Context, ch chan kafka.Message) {
	cacheId := uuid.NewString()
	// cache file, where msgs are cached
	cachePath := fmt.Sprintf("%s/cache_%s.cache", GlbConfig.Data.DataDir, cacheId)
	// cache idx file, in which each msg size is record line by line
	idxPath := fmt.Sprintf("%s/cache_%s.idx", GlbConfig.Data.DataDir, cacheId)
	// cache file lock, represents whether the cache file with the same cacheId is used
	lockPath := fmt.Sprintf("%s/cache_%s.lock", GlbConfig.Data.DataDir, cacheId)

	cacheFile, err := os.OpenFile(cachePath, os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		ServerLogger.Errorf(ctx, err, "create cache file failed")
		return
	}

	defer cacheFile.Close()

	idxFile, err := os.OpenFile(idxPath, os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		ServerLogger.Errorf(ctx, err, "create idx file failed")
		return
	}

	defer idxFile.Close()

	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		ServerLogger.Errorf(ctx, err, "create lock file failed")
		return
	}

	defer func() {
		lockFile.Close()
		os.Remove(lockPath)
	}()

	lockFile.WriteString(cacheId)

	for m := range ch {
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
}

// RushStageData rush to save data to local file before dying
func RushStageData(ctx context.Context) {
	ServerLogger.Infof(ctx, "server is stating data...")
	if len(ChMsg) == 0 && len(ChCache) == 0 {
		// no msgs in channel
		return
	}
	// msgs in channel aren't read completely,turn to save to local
	ChDirection <- RushStage
	if len(ChMsg) > 0 {
		saveMsgToLocal(ctx, ChMsg)
	}
	if len(ChCache) > 0 {
		saveMsgToLocal(ctx, ChCache)
	}
	close(ChDirection)
	ServerLogger.Infof(ctx, "server finished stating data.")
}
