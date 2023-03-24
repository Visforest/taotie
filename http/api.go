package http

import (
	"github.com/gin-gonic/gin"
	. "github.com/visforest/vftt/server"
)

const (
	Ok = iota
	BadParam
	ServerErr
)

type IntakeDataParam struct {
	Topic string `json:"topic"`
	Data  map[string]interface{} `json:"data"`
}

type BatchIntakeDataParam struct {
	Topic string `json:"topic"`
	Data  []map[string]interface{} `json:"data"`
}

type MixIntakeDataParam struct {
	Data []IntakeDataParam `json:"data"`
}

type Resp struct {
	Code int8 `json:"code"`
}

var okResp = Resp{Ok}
var badParamResp = Resp{BadParam}
var serverErrResp = Resp{ServerErr}

func IntakeData(c *gin.Context) {
	var req IntakeDataParam
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(200, badParamResp)
		return
	}
	ServerLogger.Debugf(c, "intake data req:%v", req)

	c.JSON(200, okResp)
}

func BatchIntakeData(c *gin.Context) {
	var req BatchIntakeDataParam
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(200, badParamResp)
		return
	}
	ServerLogger.Debugf(c, "batch intake data req:%v", req)

	c.JSON(200, okResp)
}

func MixIntakeData(c *gin.Context) {
	var req MixIntakeDataParam
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(200, badParamResp)
		return
	}
	ServerLogger.Debugf(c, "mix intake data req:%v", req)

	c.JSON(200, okResp)
}
