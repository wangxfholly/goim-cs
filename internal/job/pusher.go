// Package job 是设计文档「整体架构」里的 Job 层：把 Logic 产出的消息
// 投递到 Comet 网关上的在线连接（生产中通常经 Kafka 异步削峰，设计文档 4.2 / 10.2）。
package job

import (
	"encoding/json"

	"github.com/wangxfholly/goim-cs/internal/comet"
	"github.com/wangxfholly/goim-cs/internal/protocol"
	"github.com/wangxfholly/goim-cs/internal/protocol/model"
)

// CometPusher 实现 logic.Pusher，把消息编码成帧推给网关连接。
type CometPusher struct {
	bm *comet.BucketManager
}

// NewCometPusher 创建推送适配器。
func NewCometPusher(bm *comet.BucketManager) *CometPusher {
	return &CometPusher{bm: bm}
}

// PushToUser 实现 logic.Pusher。
func (p *CometPusher) PushToUser(uid int64, msg *model.Message) int {
	body, _ := json.Marshal(msg)
	f := &protocol.Frame{Version: protocol.Version, Cmd: protocol.CmdPush, Body: body}
	return p.bm.PushToUser(uid, f)
}
