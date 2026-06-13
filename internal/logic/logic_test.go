package logic

import (
	"testing"

	"github.com/wangxfholly/goim-cs/internal/idgen"
	"github.com/wangxfholly/goim-cs/internal/protocol/model"
	"github.com/wangxfholly/goim-cs/internal/store"
)

// fakePusher 记录投递。
type fakePusher struct{ delivered map[int64]int }

func (f *fakePusher) PushToUser(uid int64, _ *model.Message) int {
	f.delivered[uid]++
	return 1
}

func newLogic(t *testing.T) (*Logic, *store.MemoryStore, *fakePusher) {
	sf, err := idgen.NewSnowflake(1)
	if err != nil {
		t.Fatal(err)
	}
	ms := store.NewMemoryStore()
	fp := &fakePusher{delivered: map[int64]int{}}
	lg := New(sf, idgen.NewSeqAllocator(), ms, ms, ms, fp)
	return lg, ms, fp
}

func TestHandleSendWriteFanout(t *testing.T) {
	lg, _, fp := newLogic(t)
	_ = lg.convs.AddMember("c1", 1)
	_ = lg.convs.AddMember("c1", 2)

	msg := &model.Message{
		Envelope: model.Envelope{ConversationID: "c1", FromUID: 1, ConvType: model.ConvSingle},
		Identity: model.Identity{ClientMsgID: "uuid-1"},
		Content:  model.Content{Type: model.MsgText, Text: "hi"},
	}
	res, err := lg.HandleSend(msg)
	if err != nil {
		t.Fatal(err)
	}
	if res.ConvSeq != 1 || res.ServerMsgID == 0 {
		t.Fatalf("bad result: %+v", res)
	}
	// 写扩散：两个成员都应收到
	if fp.delivered[1] != 1 || fp.delivered[2] != 1 {
		t.Fatalf("write fanout delivery wrong: %+v", fp.delivered)
	}
}

func TestHandleSendIdempotent(t *testing.T) {
	lg, _, _ := newLogic(t)
	_ = lg.convs.AddMember("c1", 1)
	msg := &model.Message{
		Envelope: model.Envelope{ConversationID: "c1", FromUID: 1},
		Identity: model.Identity{ClientMsgID: "dup-uuid"},
	}
	_, _ = lg.HandleSend(msg)
	// 同 client_msg_id 重发
	msg2 := &model.Message{
		Envelope: model.Envelope{ConversationID: "c1", FromUID: 1},
		Identity: model.Identity{ClientMsgID: "dup-uuid"},
	}
	res, _ := lg.HandleSend(msg2)
	if !res.Duplicated {
		t.Fatal("expected duplicated=true on resend")
	}
}

func TestConvSeqMonotonic(t *testing.T) {
	lg, _, _ := newLogic(t)
	_ = lg.convs.AddMember("c1", 1)
	var last int64
	for i := 0; i < 50; i++ {
		msg := &model.Message{
			Envelope: model.Envelope{ConversationID: "c1", FromUID: 1},
			Identity: model.Identity{ClientMsgID: string(rune('a' + i))},
		}
		res, _ := lg.HandleSend(msg)
		if res.ConvSeq != last+1 {
			t.Fatalf("conv_seq not monotonic: got %d want %d", res.ConvSeq, last+1)
		}
		last = res.ConvSeq
	}
}

func TestHandleSync(t *testing.T) {
	lg, _, _ := newLogic(t)
	_ = lg.convs.AddMember("c1", 1)
	for i := 0; i < 5; i++ {
		_, _ = lg.HandleSend(&model.Message{
			Envelope: model.Envelope{ConversationID: "c1", FromUID: 1},
			Identity: model.Identity{ClientMsgID: string(rune('A' + i))},
		})
	}
	// 从 seq=2 之后补洞，应拿到 seq 3,4,5
	out, _ := lg.HandleSync("c1", 2, 100)
	if len(out) != 3 || out[0].Identity.ConvSeq != 3 {
		t.Fatalf("sync wrong: %d entries, first seq %d", len(out), out[0].Identity.ConvSeq)
	}
}
