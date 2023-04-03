package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/visforest/vftt/utils"
)

type FileCache struct {
	sync.Mutex

	id string

	cacheFilePath  string
	cacheFile      *os.File
	cacheFileBytes []byte

	idxFilePath string
	idxFile     *os.File

	lockFilePath string
	lockFile     *os.File

	readCacheAt int64
	readIdxAt   int64
	wroteAt     int
}

func NewCache() (*FileCache, error) {
	cache := &FileCache{}
	cache.id = uuid.NewString()
	// cache file, where msgs are cached
	cache.cacheFilePath = fmt.Sprintf("%s/cache_%s.cache", GlbConfig.Data.DataDir, cache.id)
	// cache idx file, in which each msg size is record line by line
	cache.idxFilePath = fmt.Sprintf("%s/cache_%s.idx", GlbConfig.Data.DataDir, cache.id)

	var err error
	// cache file, with json encoded kafka message
	cache.cacheFile, err = os.OpenFile(cache.cacheFilePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "create cache file failed")
	}
	// write placement data
	_, err = cache.cacheFile.Write(make([]byte, maxCacheSize))
	if err != nil {
		return nil, errors.Wrap(err, "read&write cache file replacement failed")
	}
	cache.cacheFileBytes, err = syscall.Mmap(int(cache.cacheFile.Fd()), 0, maxCacheSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, errors.Wrap(err, "read&write cache file failed")
	}
	// cache index file,with lengths of every json encoded kafka message
	cache.idxFile, err = os.OpenFile(cache.idxFilePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "create idx file failed")
	}

	return cache, nil
}

// Lock lock cache file
func (c *FileCache) Lock(tag string) {
	c.Mutex.Lock()
	// cache file lock, represents whether the cache file with the same cacheId is used
	c.lockFilePath = fmt.Sprintf("%s/cache_%s.lock", GlbConfig.Data.DataDir, c.id)
	// cache lock file,indicates whether the cache file is on use and prevents it from being currently modified
	c.lockFile, _ = os.OpenFile(c.lockFilePath, os.O_CREATE|os.O_WRONLY, 0666)

	c.lockFile.WriteString(tag)
	c.lockFile.Close()
}

// Unlock unlock cache file
func (c *FileCache) Unlock() error {
	err := os.Remove(c.lockFilePath)
	if err != nil {
		return err
	}
	c.Mutex.Unlock()
	return nil
}

// WriteCache continuously write cache data
func (c *FileCache) WriteCache(data []byte) {
	copy(c.cacheFileBytes[c.wroteAt:c.wroteAt+len(data)], data)
	// record json encoded kafka message length
	c.idxFile.WriteString(fmt.Sprintf("%d\n", len(data)))
	c.wroteAt += len(data)
}

// FlushCache flush cache data into cache file
func (c *FileCache) FlushCache(maxSize int) error {
	var err = syscall.Munmap(c.cacheFileBytes)
	if err != nil {
		return err
	}
	if maxSize > c.wroteAt {
		err = c.cacheFile.Truncate(int64(maxSize - c.wroteAt))
		if err != nil {
			return err
		}
	}
	err = c.cacheFile.Close()
	if err != nil {
		return err
	}
	err = c.idxFile.Close()
	if err != nil {
		return err
	}
	return err
}

// WriteToKafka read cache data, decode kafka msgs and write to kafka
func (c *FileCache) WriteToKafka(ctx context.Context) error {
	defer c.idxFile.Close()
	defer c.cacheFile.Close()
	ServerLogger.Debugf(ctx, "%s write to kafka %s", c.id, c.cacheFilePath)

	defer func() {
		ServerLogger.Debugf(ctx, "%s finish write", c.id)
		// truncate data that are already written to kafka
		err := utils.TruncateFile(c.cacheFile, c.readCacheAt)
		ServerLogger.Debugf(ctx, "%s cache file truncate from %d", c.id, c.readCacheAt)
		if err != nil {
			ServerLogger.Errorf(ctx, err, "truncate cache file failed")
		}
		err = utils.TruncateFile(c.idxFile, c.readIdxAt)
		ServerLogger.Debugf(ctx, "%s idx file truncate from %d", c.id, c.readIdxAt)
		if err != nil {
			ServerLogger.Errorf(ctx, err, "truncate idx file failed")
		}
	}()

	cacheFileInfo, _ := c.cacheFile.Stat()
	var err error
	c.cacheFileBytes, err = syscall.Mmap(int(c.cacheFile.Fd()), 0, int(cacheFileInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	idxReader := bufio.NewReader(c.idxFile)
	for {
		line, err := idxReader.ReadString('\n')
		ServerLogger.Debugf(ctx, "%s idx read %s,at %d", c.id, line, c.readIdxAt)
		if len(line) > 1 {
			msgByteLen, err := strconv.ParseInt(strings.TrimRight(line, "\n"), 10, 64)
			if err != nil {
				return err
			}
			ServerLogger.Debugf(ctx, "%s readCacheAt:%d,msgByteLen:%d", c.id, c.readCacheAt, msgByteLen)
			msgBytes := c.cacheFileBytes[c.readCacheAt : c.readCacheAt+msgByteLen]
			msgBytes = bytes.Trim(msgBytes, "\x00")
			var msg kafka.Message
			if err := json.Unmarshal(msgBytes, &msg); err != nil {
				return err
			}
			if err := KafkaWriter.WriteMessages(ctx, msg); err != nil {
				chCache <- msg
				return err
			}
			// update read progress
			c.readCacheAt += msgByteLen
			c.readIdxAt += int64(len(line))
		}
		if err == io.EOF {
			ServerLogger.Debugf(ctx, "%s read finished", c.id)
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *FileCache) Clear() error {
	var err error
	err = os.Remove(c.cacheFilePath)
	if err != nil {
		return err
	}
	err = os.Remove(c.idxFilePath)
	if err != nil {
		return err
	}
	return nil
}
