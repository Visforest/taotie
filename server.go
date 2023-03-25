package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	grpcApi "github.com/visforest/vftt/grpc"
	"github.com/visforest/vftt/grpc/proto"
	httpApi "github.com/visforest/vftt/http"
	. "github.com/visforest/vftt/server"
	"google.golang.org/grpc"
	"net"
	"os"
)

// setup HTTP service
func httpServe() {
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

	err := router.Run(GlbConfig.Server.Http.Addr)
	if err != nil {
		ServerLogger.Fatalf(context.Background(), err, "http server %s shutdown", GlbConfig.Server.Http.Addr)
	}
}

// setup grpc service
func grpcServe() {
	grpcServer := grpc.NewServer()
	pb.RegisterApiServer(grpcServer, new(grpcApi.GrpcService))
	listener, err := net.Listen("tcp", GlbConfig.Server.Grpc.Addr)
	if err != nil {
		ServerLogger.Fatalf(context.Background(), err, "grpc server listen failed")
		return
	}
	defer listener.Close()
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
	go httpServe()
	go grpcServe()
	go CacheMsg(ctx)
	go WriteMsg(ctx)
	go RewriteMsg(ctx)
	go Sentinel(ctx)

	select {
	case sig := <-ch:
		if sig == os.Kill || sig == os.Interrupt {
			ServerLogger.Fatalf(ctx, nil, "server stopped,sig is %d", sig)
			break
		}
	}
}
