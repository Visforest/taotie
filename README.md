饕餮（tāo tiè）

高并发、高可用、高吞吐量的数据摄取服务，数据摄取到 Kafka 中。

支持 http 和 gRPC 两种方式上报；
支持 pprof 性能分析；
高可用，不丢数据；

示例使用场景：

- 客户端用户行为日志上报
- 客户端广告行为日志上报
- 服务器性能监测上报
- 移动设备信息动态采集

# 执行

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
$ docker run -d --name taotie -v /tmp/vftt/cache:/data/cache/ -v /tmp/vftt/logs:/data/logs/ -v /etc/vftt/:/data/conf/ -p 8000:8000 -p 9000:9000  vftt:latest
```

## nginx部署

配置文件参阅 [nginx_example.conf](deploy/nginx_example.conf)。

# 请求示例

## 单数据上报

```curl
curl -X POST \
  http://127.0.0.1:8000/intake/one \
  -H 'Content-Type: application/json' \
  -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36 Edg/108.0.1' \
  -H 'Cookie: xxxx' \
  -H 'X-Request-ID: da6ca7b0-cbe6-11ed-97a3-acde48001122' \
  -d '{
    "topic": "user-behavior",
    "data": {
        "uid": 1024,
        "page": "/news/543425",
        "event": "click",
        "duration": 0,
        "timestamp": 1679811100990
    }
}'
```

## 同 topic 数据批量上报

```curl
curl -X POST \
  http://127.0.0.1:8000/intake/batch \
  -H 'Content-Type: application/json' \
  -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36 Edg/108.0.1' \
  -H 'Cookie: xxxx' \
  -H 'X-Request-ID: da6ca7b0-cbe6-11ed-97a3-acde48001122' \
  -d '{
    "topic": "user-behavior",
    "data": [
        {
            "uid": 1025,
            "page": "/news/543400",
            "event": "click",
            "duration": 0,
            "timestamp": 1679811100990
        },
        {
            "uid": 1025,
            "page": "/news/543400",
            "event": "stay",
            "duration": 4000,
            "timestamp": 1679811109090
        }
    ]
}'
```

## 混合 topic 数据批量上报
```curl
curl -X POST \
  http://127.0.0.1:8000/intake/mix \
  -H 'Content-Type: application/json' \
  -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36 Edg/108.0.1' \
  -H 'Cookie: xxxx' \
  -H 'X-Request-ID: da6ca7b0-cbe6-11ed-97a3-acde48001122' \
  -d '{
    "data": [
        {
            "topic": "user-behavior",
            "data": {
                "uid": 1025,
                "page": "/news/543400",
                "event": "click",
                "duration": 0,
                "timestamp": 1679811100990
            }
        },
        {
            "topic": "user-location",
            "data": {
                "lon": 110.4543,
                "lat": 42.9942,
                "timestamp": 1679811109090
            }
        }
    ]
}'
```

# 配置说明

kafka 请设置 topic 自动创建。

示例可参阅 [config_example.yaml](./config_example.yaml)：

| 字段                | 可选项  | 说明                                                                    | 默认值             |
| ------------------- | ------- | ----------------------------------------------------------------------- | ------------------ |
| server.http.addr    |         | http 服务监听地址                                                       | 127.0.0.1:8000     |
| server.grpc.addr    |         | grpc 服务监听地址                                                       | 127.0.0.1:9000     |
| server.ext_fields   |         | 数据扩展字段                                                            | 空                 |
|                     | ip      | 请求来源 IP                                                             |                    |
|                     | ua      | 请求客户端标识，即请求头中的 `User-Agent`                             |                    |
|                     | rid     | 请求标识，即请求头中的 `X-Request-ID`                                 | 使用 uuid 生成     |
|                     | ck      | 请求用户标识，即请求头中的 `Cookie`                                   |                    |
|                     | ts      | 数据业务毫秒时间戳，即数据中的 `timestamp` 字段                       | 使用当前毫秒时间戳 |
| kafka.broker        |         | kafka 连接节点地址                                                      | localhost:9092     |
| kafka.partition_cnt |         | kafka topic 分区数量                                                    | 1                  |
| kafka.write_timeout |         | kafka 写消息超时时间，单位为毫秒                                        | 10                 |
| kafka.ack_policy    |         | kafka 写入消息成功的确认机制                                            | one                |
|                     | none    | 不需要 kafka broker 确认成功，性能最好，安全性最差                      |                    |
|                     | one     | 至少有一个 kafka broker 确认成功，性能折中，安全性中等                  |                    |
|                     | all     | 所有 kafka broker 都必须确认成功，性能最差，安全性最高                  |                    |
| data.data_dir       |         | 消息本地暂存目录                                                        | ./data/            |
| log.log_dir         |         | 日志目录                                                                | ./logs/            |
| log.log_level       |         | 日志记录级别                                                            | disable            |
|                     | disable | 禁用日志                                                                |                    |
|                     | disable |                                                                         |                    |
|                     | info    |                                                                         |                    |
|                     | warn    |                                                                         |                    |
|                     | err     |                                                                         |                    |
|                     | fatal   |                                                                         |                    |
|                     | disable |                                                                         |                    |