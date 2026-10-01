package velocache

import (
	"bytes"
	"fmt"
	"testing"
)

func TestSenderLimits(t *testing.T) {
	s, err := NewSender(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Submit("big", make([]byte, MaxTransferSize+1)); err != ErrTransferTooLarge {
		t.Fatalf("expected ErrTransferTooLarge, got %v", err)
	}
	if err := s.Submit("empty", nil); err != ErrEmptyTransfer {
		t.Fatalf("expected ErrEmptyTransfer, got %v", err)
	}
	for i := 0; i < MaxInFlight; i++ {
		if err := s.Submit(fmt.Sprintf("t%02d", i), []byte("x")); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	if err := s.Submit("one-more", []byte("x")); err != ErrTooManyTransfers {
		t.Fatalf("expected ErrTooManyTransfers, got %v", err)
	}
	if err := s.Submit("t00", []byte("y")); err != ErrDuplicateID {
		t.Fatalf("expected ErrDuplicateID, got %v", err)
	}
	if n := s.InFlight(); n != MaxInFlight {
		t.Fatalf("expected %d in flight, got %d", MaxInFlight, n)
	}
	// Acknowledging one transfer frees its slot and its spooled original.
	ack := message{typ: msgAck, id: "t00"}.encode()
	if err := s.Deliver(ack); err != nil {
		t.Fatal(err)
	}
	if err := s.Submit("t00", []byte("y")); err != nil {
		t.Fatalf("resubmit after ack: %v", err)
	}
	// Late duplicate ack for an already-removed transfer is ignored.
	if err := s.Deliver(ack); err != nil {
		t.Fatal(err)
	}
}

// Duplicate messages within one transfer must not refresh LRU recency.
func TestDuplicateMessagesDoNotRefreshCache(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSender(dir + "/sender")
	if err != nil {
		t.Fatal(err)
	}
	r, err := newReceiverWithCapacity(dir+"/receiver", 2*ChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	chunkA := randomBytes(t, ChunkSize)
	chunkB := randomBytes(t, ChunkSize)
	chunkC := randomBytes(t, ChunkSize)
	hashA := hashChunk(chunkA)

	// t1 = A+B: both cached, A older than B.
	if err := s.Submit("t1", append(append([]byte{}, chunkA...), chunkB...)); err != nil {
		t.Fatal(err)
	}
	msgs := s.Pending()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 data messages, got %d", len(msgs))
	}
	for _, m := range msgs {
		if err := r.Deliver(m); err != nil {
			t.Fatal(err)
		}
	}
	// Duplicate delivery of A's message: must not refresh A's recency.
	if err := r.Deliver(msgs[0]); err != nil {
		t.Fatal(err)
	}
	if err := r.Deliver(msgs[0]); err != nil {
		t.Fatal(err)
	}

	// t2 = C: inserting C must evict A (oldest), not B.
	if err := s.Submit("t2", chunkC); err != nil {
		t.Fatal(err)
	}
	pump(t, s, r, nil, nil)
	if r.cache.has(hashA) {
		t.Fatal("duplicate messages refreshed A; expected A to be evicted")
	}
	if !r.cache.has(hashChunk(chunkB)) || !r.cache.has(hashChunk(chunkC)) {
		t.Fatal("expected B and C to remain cached")
	}

	// t3 = A again: reference misses, fill-in restores it.
	if err := s.Submit("t3", chunkA); err != nil {
		t.Fatal(err)
	}
	stats := map[msgType]int{}
	pump(t, s, r, nil, stats)
	if stats[msgNeed] == 0 || stats[msgData] == 0 {
		t.Fatalf("expected fill-in round trip, got %v", stats)
	}
	got := map[string][]byte{}
	takeAll(t, r, got)
	if !bytes.Equal(got["t3"], chunkA) {
		t.Fatal("t3 corrupted")
	}
	assertQuiet(t, s, r)
}

func TestResultsDrainedOnce(t *testing.T) {
	s, r := newPair(t)
	data := randomBytes(t, 3*ChunkSize+1)
	if err := s.Submit("once", data); err != nil {
		t.Fatal(err)
	}
	pump(t, s, r, nil, nil)
	first := r.TakeResults()
	if len(first) != 1 || !bytes.Equal(first[0].Data, data) {
		t.Fatalf("bad first drain: %d results", len(first))
	}
	if again := r.TakeResults(); len(again) != 0 {
		t.Fatalf("results drained twice: %d", len(again))
	}
	assertQuiet(t, s, r)
}
