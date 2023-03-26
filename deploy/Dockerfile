FROM golang:1.20 AS builder

COPY . /src
WORKDIR /src

RUN GOPROXY=https://goproxy.cn make build

FROM debian:stable-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates netbase \
    && rm -rf /var/lib/apt/lists/ \
    && apt-get autoremove -y \
    && apt-get autoclean -y

COPY --from=builder /src/bin /app

WORKDIR /app

VOLUME /data/conf
VOLUME /data/cache
VOLUME /data/logs

CMD ["./vftt","-c","/data/conf/config.yaml"]
