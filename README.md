饕餮（tāo tiè）

高并发、高可用、高吞吐量的数据摄取服务，数据摄取到 Kafka 中。

支持 http 和 gRPC 两种方式上报；
支持 pprof 性能分析；
支持日志按日期和文件大小分割；
高可用，不丢数据；


示例使用场景：
- 客户端用户行为日志上报
- 客户端广告行为日志上报 
- 服务器性能监测上报
- 移动设备信息动态采集

# 启动

## 编译执行

查看编译帮助：
```
$ make help
```

编译：
```
$ make linux
```

运行：
```console
$  ./bin/vftt -c your_config.yaml
```

## docker

构建镜像
```
$ docker build -f deploy/Dockerfile -t vftt .
```

启动容器：
```
$ docker run -d --name taotie -v /tmp/vftt/logs:/data/cache/ -v /etc/vftt/:/data/conf/ -p 8000:8000 -p 9000:9000  vftt:latest
```

# 配置说明

示例可参阅 [config_example.yaml](./config_example.yaml)：

| 字段                | 可选项  | 说明                                                         | 默认值             |
| ------------------- | ------- | ------------------------------------------------------------ | ------------------ |
| server.http.addr    |         | http 服务监听地址                                            | 127.0.0.1:8000     |
| server.grpc.addr    |         | grpc 服务监听地址                                            | 127.0.0.1:9000     |
| server.ext_fields   |         | 数据扩展字段                                                 | 空                 |
|                     | ip      | 请求来源 IP                                                  |                    |
|                     | ua      | 请求客户端标识，即请求头中的 `User-Agent`                    |                    |
|                     | rid     | 请求标识，即请求头中的 `X-Request-ID`                        | 使用 uuid 生成     |
|                     | ck      | 请求用户标识，即请求头中的 `Cookie`                          |                    |
|                     | ts      | 数据业务毫秒时间戳，即数据中的 `timestamp` 字段              | 使用当前毫秒时间戳 |
| kafka.broker        |         | kafka 连接节点地址                                           | localhost:9092    |
| kafka.partition_cnt |         | kafka topic 分区数量                                         | 1                  |
| kafka.write_timeout |         | kafka 写消息超时时间，单位为毫秒                             | 10                 |
| kafka.ack_policy    |         | kafka 写入消息成功的确认机制                                 | one                |
|                     | none    | 不需要 kafka broker 确认成功，性能最好，安全性最差           |                    |
|                     | one     | 至少有一个 kafka broker 确认成功，性能折中，安全性中等       |                    |
|                     | all     | 所有 kafka broker 都必须确认成功，性能最差，安全性最高       |                    |
| data.data_dir       |         | 消息本地暂存目录                                             | ./data/            |
| log.log_dir         |         | 日志目录                                                     | ./logs/            |
| log.log_level       |         | 日志记录级别                                                 | disable            |
|                     | disable | 禁用日志                                                     |                    |
|                     | disable |                                                              |                    |
|                     | info    |                                                              |                    |
|                     | warn    |                                                              |                    |
|                     | err     |                                                              |                    |
|                     | fatal   |                                                              |                    |
|                     | disable |                                                              |                    |
| log.max_size        |         | 日志按天分割后再次按文件大小分割的阈值，支持 M 和 G 两种单位，0为不分割 | 512M               |

