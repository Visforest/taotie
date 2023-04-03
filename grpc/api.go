package grpc

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	pb "github.com/visforest/vftt/grpc/proto"
	. "github.com/visforest/vftt/server"
	"github.com/visforest/vftt/utils"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type GrpcService struct {
	pb.UnimplementedApiServer
}

func patchExt(ctx context.Context, obj string) (*map[string]interface{}, error) {
	var result map[string]interface{}
	err := json.Unmarshal([]byte(obj), &result)
	if err != nil {
		return nil, err
	}
	for _, extField := range GlbConfig.Server.ExtFields {
		switch extField {
		case EXT_IP:
			result["ip"] = utils.GetIpFromGrpc(ctx)
		case EXT_EVENT_TIMESTAMP:
			if _, ok := result["timestamp"]; !ok {
				// 没有时间戳时，补足时间戳
				result["timestamp"] = time.Now().UnixMilli()
			}
		}
	}
	return &result, nil
}

func patchExts(ctx context.Context, list string) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	err := json.Unmarshal([]byte(list), &result)
	if err != nil {
		return nil, err
	}
	for _, data := range result {
		for _, extField := range GlbConfig.Server.ExtFields {
			switch extField {
			case EXT_IP:
				if md, ok := metadata.FromIncomingContext(ctx); ok {
					// 在有网关代理的情况下，获取真实 IP
					v := md.Get("X-Real-IP")
					if len(v) > 0 {
						data["ip"] = v[0]
						continue
					}
				}
				// 无网关代理的情况下获取对端的 IP
				p, ok := peer.FromContext(ctx)
				if ok {
					data["ip"] = p.Addr.String()
				}
			case EXT_EVENT_TIMESTAMP:
				if _, ok := data["timestamp"]; !ok {
					// 没有时间戳时，补足时间戳
					data["timestamp"] = time.Now().UnixMilli()
				}
			}
		}
	}
	return result, nil
}

func (s *GrpcService) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{Data: "pong"}, nil
}

func (s *GrpcService) IntakeData(ctx context.Context, req *pb.IntakeDataRequest) (*pb.IntakeDataResponse, error) {
	data, err := patchExt(ctx, req.DataObj)
	if err != nil {
		return nil, err
	}
	msg, err := GenMessage(req.Topic, data)
	if err != nil {
		return nil, err
	}
	BufMsg(msg)
	return &pb.IntakeDataResponse{}, nil
}

func (s *GrpcService) BatchIntakeData(ctx context.Context, req *pb.BatchIntakeDataRequest) (*pb.BatchIntakeDataResponse, error) {
	data, err := patchExts(ctx, req.DataList)
	if err != nil {
		return nil, err
	}
	var msgs []*kafka.Message
	for _, d := range data {
		msg, err := GenMessage(req.Topic, &d)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	BufMsg(msgs...)
	return &pb.BatchIntakeDataResponse{}, nil
}

func (s *GrpcService) MixIntakeData(ctx context.Context, req *pb.MixIntakeDataRequest) (*pb.MixIntakeDataResponse, error) {
	for _, dataItem := range req.Data {
		data, err := patchExt(ctx, dataItem.DataObj)
		if err != nil {
			return nil, err
		}
		msg, err := GenMessage(dataItem.Topic, data)
		if err != nil {
			return nil, err
		}
		BufMsg(msg)
	}
	return &pb.MixIntakeDataResponse{}, nil
}
