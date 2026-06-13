// Package protocol 实现设计文档「三、协议与 TCP/IP 要点」中定义的自定义二进制帧协议。
//
// 帧格式（大端序）:
//
//	+--------+---------+--------+--------+----------+------------------+
//	| magic  | version |  cmd   |  seq   | body_len |       body       |
//	| 2 byte | 1 byte  | 2 byte | 4 byte |  4 byte  |   body_len byte  |
//	+--------+---------+--------+--------+----------+------------------+
//
// 通过定长头部 + 变长 body 的 length-prefix 方式解决 TCP 粘包/拆包。
package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	// Magic 帧魔数，用于快速校验与协议探测。
	Magic uint16 = 0xC5C5
	// Version 当前协议版本。
	Version uint8 = 1

	// HeaderSize 固定头部长度 = 2+1+2+4+4。
	HeaderSize = 13
	// MaxBodySize 单帧 body 上限（4MB），防止恶意超大包打爆内存。
	MaxBodySize = 4 << 20
)

// 命令字 cmd 定义。客户端 <-> 网关 <-> 逻辑层共用。
const (
	CmdAuth      uint16 = 1 // 鉴权握手
	CmdAuthReply uint16 = 2
	CmdHeartbeat uint16 = 3 // 心跳保活
	CmdHeartbeatReply uint16 = 4
	CmdSend      uint16 = 5 // 上行：发送消息
	CmdSendAck   uint16 = 6 // 下行：发送 ACK（携带 server_msg_id / conv_seq）
	CmdPush      uint16 = 7 // 下行：服务端推送消息
	CmdPushAck   uint16 = 8 // 上行：客户端确认收到（用于可达性）
	CmdSync      uint16 = 9 // 上行：增量同步/补洞（带 last_seq）
)

var (
	// ErrMagicMismatch 魔数不匹配。
	ErrMagicMismatch = errors.New("protocol: magic mismatch")
	// ErrBodyTooLarge body 超过上限。
	ErrBodyTooLarge = errors.New("protocol: body too large")
)

// Frame 是一个完整的协议帧。
type Frame struct {
	Version uint8
	Cmd     uint16
	Seq     uint32 // 客户端自增序号，用于请求/响应配对
	Body    []byte // 通常是 Protobuf 编码后的消息体
}

// Encode 将帧序列化为字节流。
func (f *Frame) Encode() []byte {
	buf := make([]byte, HeaderSize+len(f.Body))
	binary.BigEndian.PutUint16(buf[0:2], Magic)
	buf[2] = f.Version
	binary.BigEndian.PutUint16(buf[3:5], f.Cmd)
	binary.BigEndian.PutUint32(buf[5:9], f.Seq)
	binary.BigEndian.PutUint32(buf[9:13], uint32(len(f.Body)))
	copy(buf[HeaderSize:], f.Body)
	return buf
}

// ReadFrame 从 r 中读取并解析一个完整帧，自动处理粘包/拆包。
//
// 实现要点（对应设计文档 3.2）:
//  1. 先读满 13 字节定长头部，拿到 body_len；
//  2. 校验 magic 与 body_len 上限；
//  3. 再 io.ReadFull 读满 body，保证一次返回一个完整逻辑帧。
func ReadFrame(r io.Reader) (*Frame, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint16(header[0:2]) != Magic {
		return nil, ErrMagicMismatch
	}
	bodyLen := binary.BigEndian.Uint32(header[9:13])
	if bodyLen > MaxBodySize {
		return nil, ErrBodyTooLarge
	}
	f := &Frame{
		Version: header[2],
		Cmd:     binary.BigEndian.Uint16(header[3:5]),
		Seq:     binary.BigEndian.Uint32(header[5:9]),
	}
	if bodyLen > 0 {
		f.Body = make([]byte, bodyLen)
		if _, err := io.ReadFull(r, f.Body); err != nil {
			return nil, err
		}
	}
	return f, nil
}
