package server

import (
	"gopkg.in/yaml.v3"
	"os"
)

type HttpConfig struct {
	Addr string `yaml:"addr"` // http server listening address,eg: 0.0.0.0:8000
}

type GrpcConfig struct {
	Addr string `yaml:"addr"` // grpc server listening address,eg: 0.0.0.0:8000
}

type ServerConfig struct {
	Http      HttpConfig `yaml:"http"`
	Grpc      GrpcConfig `yaml:"grpc"`
	ExtFields []string   `yaml:"ext_fields"` // ext fields to patch into kafka data
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
	Server ServerConfig `yaml:"server"`
	Kafka  KafkaConfig  `yaml:"kafka"`
	Data   DataConfig   `yaml:"data"`
	Log    LogConfig    `yaml:"log"`
}

func ParseConfig(file *string) error {
	f, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(f, &GlbConfig); err != nil {
		return err
	}
	return nil
}

var GlbConfig Config
