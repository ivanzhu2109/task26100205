package velocache

import (
	"bytes"
	"math/rand"
	"testing"
)

// harness wires a Sender and a Receiver together with explicit, countable
// message delivery. The tests decide when messages are delivered, in which
// order, and how many times.
type harness struct {
	t        *testing.T
	s        *Sender
	r        *Receiver
	bytes    int64
	msgCount map[byte]int
	results  map[uint64]int // id -> times delivered
	savedS2R [][]byte
	savedR2S [][]byte
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	s, err := NewSender(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReceiver(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &harness{
		t:        t,
		s:        s,
		r:        r,
		msgCount: make(map[byte]int),
		results:  make(map[uint64]int),
	}
}

func (h *harness) toReceiver(m []byte) {
	h.bytes += int64(len(m))
	h.msgCount[m[0]]++
	h.savedS2R = append(h.savedS2R, append([]byte(nil), m...))
	h.r.Deliver(m)
}

func (h *harness) toSender(m []byte) {
	h.bytes += int64(len(m))
	h.msgCount[m[0]]++
	h.savedR2S = append(h.savedR2S, append([]byte(nil), m...))
	h.s.Deliver(m)
}

// pump delivers messages until both outboxes are empty. With shuffle the
// order of each batch is randomized; with dup > 1 every message is delivered
// dup times.
func (h *harness) pump(shuffle bool, dup int, rng *rand.Rand) {
	h.t.Helper()
	for {
		var s2r, r2s [][]byte
		for {
			m, ok := h.s.NextMessage()
			if !ok {
				break
			}
			s2r = append(s2r, m)
		}
		for {
			m, ok := h.r.NextMessage()
			if !ok {
				break
			}
			r2s = append(r2s, m)
		}
		if len(s2r) == 0 && len(r2s) == 0 {
			return
		}
		if shuffle {
			rng.Shuffle(len(s2r), func(i, j int) { s2r[i], s2r[j] = s2r[j], s2r[i] })
			rng.Shuffle(len(r2s), func(i, j int) { r2s[i], r2s[j] = r2s[j], r2s[i] })
		}
		for _, m := range s2r {
			for k := 0; k < dup; k++ {
				h.toReceiver(m)
			}
		}
		for _, m := range r2s {
			for k := 0; k < dup; k++ {
				h.toSender(m)
			}
		}
	}
}

// expectResult pops exactly one result and checks it byte-for-byte.
func (h *harness) expectResult(wantID uint64, want []byte) {
	h.t.Helper()
	id, data, ok := h.r.NextResult()
	if !ok {
		h.t.Fatalf("no result available, want id %d", wantID)
	}
	if id != wantID {
		h.t.Fatalf("result id = %d, want %d", id, wantID)
	}
	h.results[id]++
	if !bytes.Equal(data, want) {
		h.t.Fatalf("result %d differs from submitted original (%d vs %d bytes)", id, len(data), len(want))
	}
}

// expectIdle asserts no messages, results, or in-flight transfers remain.
func (h *harness) expectIdle() {
	h.t.Helper()
	if _, ok := h.s.NextMessage(); ok {
		h.t.Fatal("sender outbox not empty")
	}
	if _, ok := h.r.NextMessage(); ok {
		h.t.Fatal("receiver outbox not empty")
	}
	if _, _, ok := h.r.NextResult(); ok {
		h.t.Fatal("undelivered result remains")
	}
	if len(h.s.pending) != 0 {
		h.t.Fatalf("sender still holds %d unconfirmed transfers", len(h.s.pending))
	}
	if len(h.r.live) != 0 {
		h.t.Fatalf("receiver still has %d dangling transfers", len(h.r.live))
	}
	if h.r.cache.used > h.r.cache.cap {
		h.t.Fatalf("cache over capacity: %d > %d", h.r.cache.used, h.r.cache.cap)
	}
}

func randBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	rng.Read(b)
	return b
}

func TestBasicTransfer(t *testing.T) {
	h := newHarness(t)
	rng := rand.New(rand.NewSource(1))
	data := randBytes(rng, 100_000) // not a multiple of BlockSize
	if err := h.s.Submit(7, data); err != nil {
		t.Fatal(err)
	}
	h.pump(false, 1, rng)
	h.expectResult(7, data)
	h.expectIdle()
}

// After warmup, 100 repetitions of the same 1 MiB content (128 distinct
// high-entropy blocks) must cost no more than 5% of the raw byte total on
// the wire, control messages included.
func TestRepeatTransferByteBudget(t *testing.T) {
	h := newHarness(t)
	rng := rand.New(rand.NewSource(2))
	content := randBytes(rng, 1<<20)

	// Warmup: first full transmission, excluded from the budget.
	if err := h.s.Submit(0, content); err != nil {
		t.Fatal(err)
	}
	h.pump(false, 1, rng)
	h.expectResult(0, content)

	h.bytes = 0
	h.msgCount = make(map[byte]int)
	const runs = 100
	for i := 1; i <= runs; i++ {
		id := uint64(i)
		if err := h.s.Submit(id, content); err != nil {
			t.Fatal(err)
		}
		h.pump(false, 1, rng)
		h.expectResult(id, content)
	}
	budget := int64(runs) * (1 << 20) * 5 / 100
	if h.bytes > budget {
		t.Fatalf("wire bytes %d exceed 5%% budget %d", h.bytes, budget)
	}
	t.Logf("wire bytes for %d repeats: %d (budget %d, data msgs %d, ref msgs %d)",
		runs, h.bytes, budget, h.msgCount[msgData], h.msgCount[msgRef])
	h.expectIdle()
}

// A reference is generated, the referenced block is then evicted by other
// transfers, and only afterwards does the reference arrive. The transfer
// must be repaired automatically while unrelated transfers still complete.
func TestEvictedReferenceIsRepaired(t *testing.T) {
	h := newHarness(t)
	rng := rand.New(rand.NewSource(3))

	contentX := randBytes(rng, 8*BlockSize) // 8 blocks, 64 KiB
	if err := h.s.Submit(1, contentX); err != nil {
		t.Fatal(err)
	}
	h.pump(false, 1, rng)
	h.expectResult(1, contentX)

	// Evict X from the receiver cache with > 1 MiB of fresh content.
	id := uint64(2)
	for i := 0; i < 10; i++ {
		fresh := randBytes(rng, 16*BlockSize) // 128 KiB each
		if err := h.s.Submit(id, fresh); err != nil {
			t.Fatal(err)
		}
		h.pump(false, 1, rng)
		h.expectResult(id, fresh)
		id++
	}
	if h.r.cache.used > cacheCapacity {
		t.Fatalf("cache over capacity: %d", h.r.cache.used)
	}

	// Resubmit X: the sender still believes the receiver holds the blocks
	// and emits pure references. Delay them.
	if err := h.s.Submit(id, contentX); err != nil {
		t.Fatal(err)
	}
	idX := id
	id++
	var delayed [][]byte
	for {
		m, ok := h.s.NextMessage()
		if !ok {
			break
		}
		if m[0] != msgRef {
			t.Fatalf("expected pure references for retransmission, got type %d", m[0])
		}
		delayed = append(delayed, m)
	}

	// Other transfers must complete while X's references are delayed.
	for i := 0; i < 3; i++ {
		other := randBytes(rng, 40_000)
		if err := h.s.Submit(id, other); err != nil {
			t.Fatal(err)
		}
		h.pump(false, 1, rng)
		h.expectResult(id, other)
		id++
	}
	if _, _, ok := h.r.NextResult(); ok {
		t.Fatal("transfer X completed before its references were delivered")
	}

	// Deliver the delayed references; the missing blocks must be repaired.
	needsBefore := h.msgCount[msgNeed]
	for _, m := range delayed {
		h.toReceiver(m)
	}
	h.pump(false, 1, rng)
	if h.msgCount[msgNeed] == needsBefore {
		t.Fatal("expected repair requests for evicted blocks")
	}
	h.expectResult(idX, contentX)
	h.expectIdle()
}

// Data and control messages arrive out of order and duplicated; old messages
// are redelivered after the sender has released the originals. Results must
// be delivered exactly once and nothing may hang.
func TestDisorderDuplicatesAndLateMessages(t *testing.T) {
	h := newHarness(t)
	rng := rand.New(rand.NewSource(4))

	partA := randBytes(rng, 50_000)
	partB := randBytes(rng, 30_000)
	t1 := append(append([]byte(nil), partA...), partB...) // fresh
	t2 := append(append([]byte(nil), partB...), randBytes(rng, 20_000)...) // reuses partB
	t3 := randBytes(rng, 37_000)                          // fresh
	want := map[uint64][]byte{11: t1, 12: t2, 13: t3}
	for id, data := range want {
		if err := h.s.Submit(id, data); err != nil {
			t.Fatal(err)
		}
	}

	// Everything shuffled, every message delivered twice.
	h.pump(true, 2, rng)

	got := make(map[uint64]int)
	for {
		id, data, ok := h.r.NextResult()
		if !ok {
			break
		}
		got[id]++
		if !bytes.Equal(data, want[id]) {
			t.Fatalf("result %d differs from original", id)
		}
	}
	for id := range want {
		if got[id] != 1 {
			t.Fatalf("result %d delivered %d times, want 1", id, got[id])
		}
	}
	h.expectIdle()

	// Originals are released by now. Redeliver every old message again:
	// nothing may be re-delivered, re-awaited, or re-queued.
	for _, m := range h.savedS2R {
		h.r.Deliver(m)
	}
	for _, m := range h.savedR2S {
		h.s.Deliver(m)
	}
	if _, _, ok := h.r.NextResult(); ok {
		t.Fatal("late messages caused a duplicate result")
	}
	h.expectIdle()
}
