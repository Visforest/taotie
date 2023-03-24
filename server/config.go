package server

import (
	"gopkg.in/yaml.v3"
	"os"
)

type HttpConfig struct {
	Addr string `yaml:"addr"`
}

type GrpcConfig struct {
	Addr string `yaml:"addr"`
}

type ServerConfig struct {
	Http      HttpConfig `yaml:"http"`       // http 服务配置
	Grpc      GrpcConfig `yaml:"grpc"`       // grpc 服务配置
	ExtFields []string   `yaml:"ext_fields"` // 采集扩展字段
}

type KafkaConfig struct {
	Broker       []string `yaml:"broker"`        // broker 连接地址
	PartitionCnt int      `yaml:"partition_cnt"` // topic 分区数量
	AckPolicy    uint8    `yaml:"ack_policy"`    // 向 Kafka 写入数据成功的确认策略
	WriteTimeout int      `yaml:"write_timeout"` // 写入超时时间
}

type DataConfig struct {
	DataDir string `yaml:"data_dir"` // 暂存数据目录
}

type LogConfig struct {
	LogDir   string `yaml:"log_dir"`   // 日志目录
	LogLevel string `yaml:"log_level"` // 日志记录级别
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
