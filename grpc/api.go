package grpc

import (
	"context"
	"github.com/visforest/vftt/grpc/proto"
)

type GrpcService struct {
	pb.UnimplementedApiServer
}

//func (s *GrpcService) mustEmbedUnimplementedApiServer() {
//	panic("implement me")
//}

func (s *GrpcService) IntakeData(ctx context.Context, req *pb.IntakeDataRequest) (*pb.IntakeDataResponse, error) {
	return &pb.IntakeDataResponse{}, nil
}

func (s *GrpcService) BatchIntakeData(ctx context.Context, req *pb.BatchIntakeDataRequest) (*pb.BatchIntakeDataResponse, error) {
	return &pb.BatchIntakeDataResponse{}, nil
}

func (s *GrpcService) MixIntakeData(ctx context.Context, req *pb.MixIntakeDataRequest) (*pb.MixIntakeDataResponse, error) {
	return &pb.MixIntakeDataResponse{}, nil
}
