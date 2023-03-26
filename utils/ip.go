package utils

import (
	"context"
	"net"
	"strings"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// GetIpFromGin get ip from gin request context
func GetIpFromGin(ctx *gin.Context) (ip string) {
	ip = ctx.Request.Header.Get("X-Real-IP")
	if ip == "" {
		if ctx.Request.Header.Get("X-Forwarded-For") != "" {
			ips := strings.Split(ctx.Request.Header.Get("X-Forwarded-For"), ",")
			if len(ips) > 0 {
				// 有多层代理转发的情况下，取首个 IP
				ip = ips[0]
			} else {
				ip = ctx.ClientIP()
			}
		} else {
			ip = ctx.ClientIP()
		}
	}
	return
}

// GetIpFromGrpc get ip from grpc request context
func GetIpFromGrpc(ctx context.Context) (ip string) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		// 在有网关代理的情况下，获取真实 IP
		v := md.Get("X-Real-IP")
		if len(v) > 0 {
			ip = v[0]
			return
		}
		v = md.Get("X-Forwarded-For")
		if len(v) > 0 {
			// 多层代理转发
			ip = v[0]
			return
		}
	}
	// 无网关代理的情况下获取对端的 IP
	pr, ok := peer.FromContext(ctx)
	if ok {
		if tcpAddr, ok := pr.Addr.(*net.TCPAddr); ok {
			ip = tcpAddr.IP.String()
		} else {
			ip = pr.Addr.String()
		}
	}
	return ip
}
