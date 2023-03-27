package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	grpcApi "github.com/visforest/vftt/grpc"
	pb "github.com/visforest/vftt/grpc/proto"
	httpApi "github.com/visforest/vftt/http"
	. "github.com/visforest/vftt/server"
	"google.golang.org/grpc"
)

// setup HTTP service
func httpServe(ctx context.Context) {
	router := gin.Default()
	// logic apis
	intake := router.Group("/intake")
	{
		intake.POST("/one", httpApi.IntakeData)
		intake.POST("/batch", httpApi.BatchIntakeData)
		intake.POST("/mix", httpApi.MixIntakeData)
	}
	// performance monitor apis
	pprof.Register(router, "/dev/pprof")

	ServerLogger.Debugf(ctx, "http server started...")
	err := router.Run(GlbConfig.Server.Http.Addr)
	if err != nil {
		ServerLogger.Fatalf(context.Background(), err, "http server %s shutdown", GlbConfig.Server.Http.Addr)
	}
}

// setup grpc service
func grpcServe(ctx context.Context) {
	grpcServer := grpc.NewServer()
	pb.RegisterApiServer(grpcServer, new(grpcApi.GrpcService))
	listener, err := net.Listen("tcp", GlbConfig.Server.Grpc.Addr)
	if err != nil {
		ServerLogger.Fatalf(ctx, err, "grpc server listen failed")
		return
	}
	defer listener.Close()
	ServerLogger.Debugf(ctx, "grpc server started...")
	err = grpcServer.Serve(listener)
	if err != nil {
		ServerLogger.Fatalf(context.Background(), err, "grpc server %s shutdown", GlbConfig.Server.Grpc.Addr)
	}
}

func main() {
	configPath := flag.String("c", "config.yaml", "config file path,default: ./config.yaml")
	flag.Parse()

	var err error
	err = ParseConfig(configPath)
	if err != nil {
		panic(err)
	}

	fmt.Println(GlbConfig)

	ServerLogger, err = InitLogger("server.log")
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	InitKafka(ctx)

	defer RushStageData(ctx)

	ch := make(chan os.Signal)
	go httpServe(ctx)
	go grpcServe(ctx)
	go CacheMsg(ctx)
	go SendMsg(ctx)
	go ResendMsg(ctx)
	go Sentinel(ctx)

	for {
		sig := <-ch
		if sig == os.Kill || sig == os.Interrupt {
			ServerLogger.Panicf(ctx, nil, "server stopped,sig is %d", sig)
		}
	}
}
