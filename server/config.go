package server

import (
	"bufio"
	"io"
	"os"
	"strings"

	"github.com/Visforest/goset"
	"gopkg.in/yaml.v3"
)

type HttpConfig struct {
	Addr string `yaml:"addr"` // http server listening address,eg: 0.0.0.0:8000
}

type GrpcConfig struct {
	Addr string `yaml:"addr"` // grpc server listening address,eg: 0.0.0.0:8000
}

type ServerConfig struct {
	Http                 HttpConfig `yaml:"http"`
	Grpc                 GrpcConfig `yaml:"grpc"`
	ExtFields            []string   `yaml:"ext_fields"`      // ext fields to patch into kafka data
	TopicFile            string     `yaml:"topic_whitelist"` // topic whitelist file
	EnableTopicWhitelist bool
	TopicWhitelist       *goset.Set
}

type KafkaConfig struct {
	Broker       []string `yaml:"broker"`        // broker addresses
	PartitionCnt int      `yaml:"partition_cnt"` // topic partition count
	AckPolicy    int      `yaml:"ack_policy"`    // acknowledgement policy for writing msg successfully
	WriteTimeout int      `yaml:"write_timeout"` // write timeout, million second unit
}

type DataConfig struct {
	DataDir string `yaml:"data_dir"` // cache data directory
}

type LogConfig struct {
	LogDir   string `yaml:"log_dir"`
	LogLevel string `yaml:"log_level"`
}

type Config struct {
	Server ServerConfig  `yaml:"server"`
	Kafka  KafkaConfig   `yaml:"kafka"`
	Data   DataConfig    `yaml:"data"`
	Log    TLoggerConfig `yaml:"log"`
}

func MustLoadConfig(file *string) {
	f, err := os.ReadFile(*file)
	if err != nil {
		panic(err)
	}
	if err := yaml.Unmarshal(f, &GlbConfig); err != nil {
		panic(err)
	}
	if GlbConfig.Server.TopicFile != "" {
		topicFile, err := os.Open(GlbConfig.Server.TopicFile)
		if err != nil {
			panic(err)
		}
		GlbConfig.Server.TopicWhitelist = goset.NewSet()
		GlbConfig.Server.EnableTopicWhitelist = true
		topicReader := bufio.NewReader(topicFile)
		for {
			line, err := topicReader.ReadString('\n')
			line = strings.Trim(line, "\n")
			line = strings.Trim(line, " ")
			if line != "" {
				GlbConfig.Server.TopicWhitelist.Add(line)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}
		}
	}
}

var GlbConfig Config
