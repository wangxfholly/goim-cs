// Command demo 串起 Comet 网关 + Logic 路由 + Store + IDGen，
// 在本机用真实 TCP 跑通「单聊收发 + 服务端分配 ID/seq + 在线推送」全链路，
// 用于验证设计文档第二/四/五章的端到端正确性。
//
// 运行: go run ./cmd/demo
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/wangxfholly/goim-cs/internal/comet"
	"github.com/wangxfholly/goim-cs/internal/idgen"
	"github.com/wangxfholly/goim-cs/internal/job"
	"github.com/wangxfholly/goim-cs/internal/logic"
	"github.com/wangxfholly/goim-cs/internal/protocol"
	"github.com/wangxfholly/goim-cs/internal/protocol/model"
	"github.com/wangxfholly/goim-cs/internal/store"
)

// handler 把网关上行帧接到 Logic。
type handler struct {
	lg *logic.Logic
}

func (h *handler) OnAuth(_ int64, body []byte) (int64, []byte, error) {
	// demo 鉴权：body 前 8 字节是 uid。
	uid := int64(binary.BigEndian.Uint64(body))
	return uid, []byte("ok"), nil
}

func (h *handler) OnMessage(c *comet.Conn, f *protocol.Frame) {
	if f.Cmd != protocol.CmdSend {
		return
	}
	var msg model.Message
	if err := json.Unmarshal(f.Body, &msg); err != nil {
		return
	}
	res, err := h.lg.HandleSend(&msg)
	if err != nil {
		log.Printf("HandleSend error: %v", err)
		return
	}
	// 回发 ACK（携带 server_msg_id / conv_seq）
	ackBody, _ := json.Marshal(res)
	c.Push(&protocol.Frame{Version: protocol.Version, Cmd: protocol.CmdSendAck, Seq: f.Seq, Body: ackBody})
}

func main() {
	// 1. 组装依赖（对应设计文档整体架构分层）。
	//    解初始化环: 先建 Logic(pusher=nil) → handler → 网关 → 用网关 Buckets 构造 Pusher 回注。
	sf, _ := idgen.NewSnowflake(1)
	ms := store.NewMemoryStore()
	lg := logic.New(sf, idgen.NewSeqAllocator(), ms, ms, ms, nil)
	h := &handler{lg: lg}
	srv := comet.NewServer(comet.Config{Buckets: 64}, h)
	lg.SetPusher(job.NewCometPusher(srv.Buckets()))

	// 2. 建会话：alice(1) <-> bob(2) 单聊。
	_ = ms.AddMember("c-ab", 1)
	_ = ms.AddMember("c-ab", 2)

	// 3. 起网关。
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	addr := ln.Addr().String()
	go srv.Serve(ln)
	fmt.Println("[demo] gateway listening on", addr)

	// 4. bob 上线接收。
	bobRecv := make(chan *model.Message, 4)
	go dialAndAuth(addr, 2, func(c net.Conn) {
		for {
			f, err := protocol.ReadFrame(c)
			if err != nil {
				return
			}
			if f.Cmd == protocol.CmdPush {
				var m model.Message
				_ = json.Unmarshal(f.Body, &m)
				bobRecv <- &m
			}
		}
	})

	// 5. alice 上线并发一条消息。
	time.Sleep(200 * time.Millisecond)
	dialAndAuth(addr, 1, func(c net.Conn) {
		msg := model.Message{
			Envelope: model.Envelope{ConversationID: "c-ab", FromUID: 1, ConvType: model.ConvSingle, SenderRole: model.RoleUser},
			Identity: model.Identity{ClientMsgID: "uuid-alice-1"},
			Content:  model.Content{Type: model.MsgText, Text: "Hello Bob!"},
		}
		body, _ := json.Marshal(msg)
		_, _ = c.Write((&protocol.Frame{Version: protocol.Version, Cmd: protocol.CmdSend, Seq: 1, Body: body}).Encode())

		// 读 ACK
		f, _ := protocol.ReadFrame(c)
		var res logic.SendResult
		_ = json.Unmarshal(f.Body, &res)
		fmt.Printf("[demo] alice got ACK: server_msg_id=%d conv_seq=%d\n", res.ServerMsgID, res.ConvSeq)
	})

	// 6. 校验 bob 收到。
	select {
	case m := <-bobRecv:
		fmt.Printf("[demo] bob received: %q (conv_seq=%d, from=%d)\n",
			m.Content.Text, m.Identity.ConvSeq, m.Envelope.FromUID)
		fmt.Println("[demo] END-TO-END OK ✓")
	case <-time.After(2 * time.Second):
		log.Fatal("[demo] timeout: bob did not receive message")
	}
}

// dialAndAuth 建连 + 鉴权 + 回调（回调中持续使用连接）。
func dialAndAuth(addr string, uid int64, fn func(net.Conn)) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	authBody := make([]byte, 8)
	binary.BigEndian.PutUint64(authBody, uint64(uid))
	_, _ = c.Write((&protocol.Frame{Version: protocol.Version, Cmd: protocol.CmdAuth, Seq: 1, Body: authBody}).Encode())
	_, _ = protocol.ReadFrame(c) // auth reply
	fn(c)
}
