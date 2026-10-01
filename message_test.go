package velocache

import (
	"bytes"
	"reflect"
	"testing"
)

func TestMessageRoundTrip(t *testing.T) {
	h := hashChunk([]byte("hello"))
	msgs := []message{
		{typ: msgData, id: "t-1", total: 3, index: 2, hash: h, payload: []byte("chunk-bytes")},
		{typ: msgData, id: "t-2", total: 1, index: 0, hash: h, payload: nil},
		{typ: msgRef, id: "transfer/λ", total: 512, index: 7, hash: h},
		{typ: msgNeed, id: "t-1", indices: []uint32{0, 5, 300}},
		{typ: msgNeed, id: "t-1", indices: nil},
		{typ: msgAck, id: "t-9"},
	}
	for _, in := range msgs {
		out, err := decodeMessage(in.encode())
		if err != nil {
			t.Fatalf("decode %+v: %v", in, err)
		}
		if !reflect.DeepEqual(in.id, out.id) || in.typ != out.typ ||
			in.total != out.total || in.index != out.index ||
			in.hash != out.hash ||
			!bytes.Equal(in.payload, out.payload) ||
			!reflect.DeepEqual(in.indices, out.indices) {
			t.Fatalf("round trip mismatch: in=%+v out=%+v", in, out)
		}
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	for _, b := range [][]byte{
		nil,
		{99, 0},           // unknown type
		{1, 5, 'a'},       // truncated id
		{2, 1, 'x', 0, 0}, // truncated ref body
	} {
		if _, err := decodeMessage(b); err == nil {
			t.Fatalf("expected error for %v", b)
		}
	}
}

func TestRefMessageIsSmall(t *testing.T) {
	m := message{typ: msgRef, id: "budget-transfer-id", total: 128, index: 3, hash: hashChunk([]byte("x"))}
	if n := len(m.encode()); n > 64 {
		t.Fatalf("ref message too large: %d bytes", n)
	}
}
