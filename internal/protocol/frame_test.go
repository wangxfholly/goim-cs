package protocol

import (
	"bytes"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	in := &Frame{Version: Version, Cmd: CmdSend, Seq: 42, Body: []byte("hello world")}
	raw := in.Encode()
	if len(raw) != HeaderSize+len(in.Body) {
		t.Fatalf("encoded len = %d, want %d", len(raw), HeaderSize+len(in.Body))
	}
	out, err := ReadFrame(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if out.Cmd != in.Cmd || out.Seq != in.Seq || !bytes.Equal(out.Body, in.Body) {
		t.Fatalf("round trip mismatch: %+v vs %+v", out, in)
	}
}

// TestStickyPackets 验证粘包场景：两个帧拼在一个流里，应被正确拆分。
func TestStickyPackets(t *testing.T) {
	f1 := &Frame{Version: Version, Cmd: CmdHeartbeat, Seq: 1, Body: nil}
	f2 := &Frame{Version: Version, Cmd: CmdSend, Seq: 2, Body: []byte("second")}
	stream := bytes.NewReader(append(f1.Encode(), f2.Encode()...))

	got1, err := ReadFrame(stream)
	if err != nil || got1.Seq != 1 {
		t.Fatalf("frame1: %v %+v", err, got1)
	}
	got2, err := ReadFrame(stream)
	if err != nil || got2.Seq != 2 || !bytes.Equal(got2.Body, []byte("second")) {
		t.Fatalf("frame2: %v %+v", err, got2)
	}
}

func TestMagicMismatch(t *testing.T) {
	bad := []byte{0x00, 0x00, 1, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0}
	if _, err := ReadFrame(bytes.NewReader(bad)); err != ErrMagicMismatch {
		t.Fatalf("want ErrMagicMismatch, got %v", err)
	}
}
