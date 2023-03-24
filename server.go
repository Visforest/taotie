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

// 启动 HTTP 服务
func httpServe() {
	router := gin.Default()
	intake := router.Group("/intake")
	//intake.Use(httpApi.PatchData)
	{
		intake.POST("/one", httpApi.IntakeData)
		intake.POST("/batch", httpApi.BatchIntakeData)
		intake.POST("/mix", httpApi.MixIntakeData)
	}
	// 性能监测
	pprof.Register(router, "/dev/pprof")

	err := router.Run(GlbConfig.Server.Http.Addr)
	if err != nil {
		ServerLogger.Fatalf(context.Background(), err, "http server shutdown")
	}
}

// 启动 grpc 服务
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
		ServerLogger.Fatalf(context.Background(), err, "grpc server shutdown")
	}
}

func sentinel() {

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

	ch := make(chan os.Signal)
	go httpServe()
	go grpcServe()
	go sentinel()
	select {
	case sig := <-ch:
		if sig == os.Kill || sig == os.Interrupt {
			ServerLogger.Fatalf(context.Background(), nil, "server stopped,sig is %d", sig)
			break
		}
	}
}
