package http

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	. "github.com/visforest/vftt/server"
	"time"
)

const (
	Ok = iota
	BadParam
	ServerErr
)

type IntakeDataParam struct {
	Topic string                 `json:"topic"`
	Data  map[string]interface{} `json:"data"`
}

type BatchIntakeDataParam struct {
	Topic string                   `json:"topic"`
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

// 为数据填充扩展字段
func patchExts(c *gin.Context, data *map[string]interface{}) {
	for _, extField := range GlbConfig.Server.ExtFields {
		switch extField {
		case EXT_IP:
			// 来源IP
			ip := c.Request.Header.Get("X-Forward-For")
			if ip == "" {
				ip = c.ClientIP()
			}
			(*data)["ip"] = ip
		case EXT_UA:
			// 客户端标识
			ua := c.Request.Header.Get("User-Agent")
			(*data)["ua"] = ua
		case EXT_REQUEST_ID:
			// 请求ID
			rid := c.Request.Header.Get("X-Request-ID")
			if rid == "" {
				// 生成请求 ID
				id, err := uuid.NewUUID()
				if err != nil {
					ServerLogger.Errorf(c, err, "gen uuid failed")
					continue
				}
				rid = id.String()
			}
			c.Set("rid", rid)
			(*data)["rid"] = rid
		case EXT_COOKIE:
			// cookie
			cookie := c.Request.Header.Get("Cookie")
			(*data)["cookie"] = cookie
		case EXT_EVENT_TIMESTAMP:
			// 时间时间戳
			if _, ok := (*data)["timestamp"]; !ok {
				// 添加毫秒时间戳
				(*data)["timestamp"] = time.Now().UnixMilli()
			}
		}
	}
}

func genMessage(topic string, data *map[string]interface{}) (*kafka.Message, error) {
	uuidKey, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	bytes, err := json.Marshal(*data)
	if err != nil {
		return nil, err
	}
	return &kafka.Message{
		Topic: topic,
		Key:   []byte(uuidKey.String()),
		Value: bytes,
	}, nil
}

func IntakeData(c *gin.Context) {
	var req IntakeDataParam
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(200, badParamResp)
		return
	}
	ServerLogger.Debugf(c, "intake data req:%v", req)
	if _, ok := req.Data["timestamp"]; !ok {
		// 添加号码时间戳
		req.Data["timestamp"] = time.Now().UnixMicro()
	}
	patchExts(c, &req.Data)

	msg, err := genMessage(req.Topic, &req.Data)
	if err != nil {
		ServerLogger.Errorf(c, err, "gen kafka msg")
		c.JSON(200, serverErrResp)
		return
	}

	BufMsg(msg)

	c.JSON(200, okResp)
}

func BatchIntakeData(c *gin.Context) {
	var req BatchIntakeDataParam
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(200, badParamResp)
		return
	}
	ServerLogger.Debugf(c, "batch intake data req:%v", req)

	var msgs []*kafka.Message
	for _, d := range req.Data {
		tmp := d
		patchExts(c, &tmp)
		msg, err := genMessage(req.Topic, &tmp)
		if err != nil {
			ServerLogger.Errorf(c, err, "gen kafka msg")
			c.JSON(200, serverErrResp)
			return
		}
		msgs = append(msgs, msg)
	}
	BufMsg(msgs...)

	c.JSON(200, okResp)
}

func MixIntakeData(c *gin.Context) {
	var req MixIntakeDataParam
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(200, badParamResp)
		return
	}
	ServerLogger.Debugf(c, "mix intake data req:%v", req)
	var msgs []*kafka.Message
	for _, d := range req.Data {
		tmp := d
		patchExts(c, &tmp.Data)

		msg, err := genMessage(d.Topic, &d.Data)
		if err != nil {
			ServerLogger.Errorf(c, err, "gen kafka msg")
			c.JSON(200, serverErrResp)
			return
		}
		msgs = append(msgs, msg)
	}
	BufMsg(msgs...)

	c.JSON(200, okResp)
}
