package velocache

import (
	"bytes"
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"path/filepath"
	"testing"
)

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func newPair(t *testing.T) (*Sender, *Receiver) {
	t.Helper()
	dir := t.TempDir()
	s, err := NewSender(filepath.Join(dir, "sender"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReceiver(filepath.Join(dir, "receiver"))
	if err != nil {
		t.Fatal(err)
	}
	return s, r
}

// pump delivers messages back and forth until both sides are quiet, counting
// bytes and message types when stats are requested.
func pump(t *testing.T, s *Sender, r *Receiver, byteCount *int64, stats map[msgType]int) {
	t.Helper()
	for {
		sm, rm := s.Pending(), r.Pending()
		if len(sm) == 0 && len(rm) == 0 {
			return
		}
		for _, m := range sm {
			account(t, m, byteCount, stats)
			if err := r.Deliver(m); err != nil {
				t.Fatal(err)
			}
		}
		for _, m := range rm {
			account(t, m, byteCount, stats)
			if err := s.Deliver(m); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func account(t *testing.T, m []byte, byteCount *int64, stats map[msgType]int) {
	t.Helper()
	if byteCount != nil {
		*byteCount += int64(len(m))
	}
	if stats != nil {
		dm, err := decodeMessage(m)
		if err != nil {
			t.Fatal(err)
		}
		stats[dm.typ]++
	}
}

// takeAll drains results into got, failing if an ID is delivered twice.
func takeAll(t *testing.T, r *Receiver, got map[string][]byte) {
	t.Helper()
	for _, res := range r.TakeResults() {
		if _, dup := got[res.ID]; dup {
			t.Fatalf("result %q delivered more than once", res.ID)
		}
		got[res.ID] = res.Data
	}
}

func assertQuiet(t *testing.T, s *Sender, r *Receiver) {
	t.Helper()
	if n := s.InFlight(); n != 0 {
		t.Fatalf("sender has %d dangling transfers", n)
	}
	if n := r.PendingCount(); n != 0 {
		t.Fatalf("receiver has %d dangling transfers", n)
	}
	if m := s.Pending(); len(m) != 0 {
		t.Fatalf("sender outbox not empty: %d messages", len(m))
	}
	if m := r.Pending(); len(m) != 0 {
		t.Fatalf("receiver outbox not empty: %d messages", len(m))
	}
}

// Scenario 1: after warmup, 100 identical 1 MiB transfers (128 distinct
// high-entropy chunks) must cost at most 5% of the raw byte total in actual
// messages, both directions, control messages included.
func TestRepeatTransfersByteBudget(t *testing.T) {
	s, r := newPair(t)
	content := randomBytes(t, 1<<20)

	chunks := splitChunks(content)
	if len(chunks) != 128 {
		t.Fatalf("expected 128 chunks, got %d", len(chunks))
	}
	seen := map[Hash]bool{}
	for _, c := range chunks {
		if seen[hashChunk(c)] {
			t.Fatal("warmup content has duplicate chunks")
		}
		seen[hashChunk(c)] = true
	}

	// Warmup (excluded from the byte budget).
	if err := s.Submit("warmup", content); err != nil {
		t.Fatal(err)
	}
	pump(t, s, r, nil, nil)
	got := map[string][]byte{}
	takeAll(t, r, got)
	if !bytes.Equal(got["warmup"], content) {
		t.Fatal("warmup transfer corrupted")
	}

	var bytesUsed int64
	stats := map[msgType]int{}
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("xfer-%03d", i)
		if err := s.Submit(id, content); err != nil {
			t.Fatal(err)
		}
		pump(t, s, r, &bytesUsed, stats)
	}
	takeAll(t, r, got)
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("xfer-%03d", i)
		data, ok := got[id]
		if !ok {
			t.Fatalf("missing result for %s", id)
		}
		if !bytes.Equal(data, content) {
			t.Fatalf("result %s does not match original", id)
		}
	}
	budget := int64(100*(1<<20)) * 5 / 100
	t.Logf("bytes used: %d (budget %d), messages: %v", bytesUsed, budget, stats)
	if bytesUsed > budget {
		t.Fatalf("message bytes %d exceed 5%% budget %d", bytesUsed, budget)
	}
	if stats[msgData] != 0 {
		t.Fatalf("expected no data messages after warmup, got %d", stats[msgData])
	}
	if stats[msgNeed] != 0 {
		t.Fatalf("expected no fill-in requests, got %d", stats[msgNeed])
	}
	assertQuiet(t, s, r)
}

// Scenario 2: a reference is generated, its chunk is then evicted by other
// transfers, and the reference arrives afterwards. The transfer must complete
// automatically via fill-in while other transfers complete normally.
func TestEvictedReferenceFillIn(t *testing.T) {
	s, r := newPair(t)
	chunkA := randomBytes(t, ChunkSize)

	// Warm the receiver cache with chunk A.
	if err := s.Submit("t1", chunkA); err != nil {
		t.Fatal(err)
	}
	pump(t, s, r, nil, nil)

	// Generate a reference to A now, but hold it back.
	if err := s.Submit("t3", chunkA); err != nil {
		t.Fatal(err)
	}
	delayed := s.Pending()
	stats := map[msgType]int{}
	for _, m := range delayed {
		account(t, m, nil, stats)
	}
	if stats[msgRef] != 1 || stats[msgData] != 0 {
		t.Fatalf("expected t3 to be a single reference, got %v", stats)
	}

	// Other transfers flow and evict A from the 1 MiB cache.
	evictor := randomBytes(t, 2<<20)
	if err := s.Submit("t2", evictor); err != nil {
		t.Fatal(err)
	}
	pump(t, s, r, nil, nil)
	got := map[string][]byte{}
	takeAll(t, r, got)
	if !bytes.Equal(got["t1"], chunkA) || !bytes.Equal(got["t2"], evictor) {
		t.Fatal("t1/t2 corrupted")
	}
	if _, ok := got["t3"]; ok {
		t.Fatal("t3 must still be waiting for its delayed reference")
	}

	// The stale reference finally arrives: cache miss, fill-in, completion.
	for _, m := range delayed {
		if err := r.Deliver(m); err != nil {
			t.Fatal(err)
		}
	}
	stats = map[msgType]int{}
	pump(t, s, r, nil, stats)
	takeAll(t, r, got)
	if stats[msgNeed] == 0 {
		t.Fatal("expected a fill-in request for the evicted chunk")
	}
	if stats[msgData] == 0 {
		t.Fatal("expected fill-in data from the sender")
	}
	if !bytes.Equal(got["t3"], chunkA) {
		t.Fatal("t3 corrupted after fill-in")
	}
	assertQuiet(t, s, r)
}

// Scenario 3: data and control messages are reordered and duplicated; after
// completion, old messages are redelivered and must not re-deliver results or
// re-open transfers.
func TestReorderDuplicateAndLateMessages(t *testing.T) {
	rng := mrand.New(mrand.NewSource(1))
	s, r := newPair(t)

	contents := map[string][]byte{
		"alpha": randomBytes(t, 100*1024+7),    // partial last chunk
		"beta":  randomBytes(t, 1<<20),         // exactly 128 chunks
		"gamma": randomBytes(t, 3*(1<<20)+123), // large, partial last chunk
		"junk":  randomBytes(t, 2<<20),         // evicts everything before it
	}
	contents["delta"] = contents["alpha"] // references; will miss after junk

	type wireMsg struct {
		toReceiver bool
		body       []byte
	}
	var history []wireMsg
	got := map[string][]byte{}

	scramble := func(q [][]byte) [][]byte {
		var out [][]byte
		for _, m := range q {
			out = append(out, m)
			if rng.Intn(3) == 0 {
				out = append(out, m) // duplicate
			}
		}
		rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
		return out
	}

	// churn delivers with reordering+duplication until quiescent.
	churn := func() {
		t.Helper()
		var qS2R, qR2S [][]byte
		for step := 0; step < 10000; step++ {
			qS2R = append(qS2R, s.Pending()...)
			qR2S = append(qR2S, r.Pending()...)
			if len(qS2R) == 0 && len(qR2S) == 0 {
				if s.InFlight() == 0 && r.PendingCount() == 0 {
					return
				}
				t.Fatalf("stuck: inflight=%d pending=%d", s.InFlight(), r.PendingCount())
			}
			qS2R, qR2S = scramble(qS2R), scramble(qR2S)
			for _, m := range qS2R {
				history = append(history, wireMsg{true, m})
				if err := r.Deliver(m); err != nil {
					t.Fatal(err)
				}
			}
			for _, m := range qR2S {
				history = append(history, wireMsg{false, m})
				if err := s.Deliver(m); err != nil {
					t.Fatal(err)
				}
			}
			qS2R, qR2S = nil, nil
			takeAll(t, r, got)
		}
		t.Fatal("did not quiesce")
	}

	// Phase 1: fresh content plus a cache-clearing junk transfer.
	for _, id := range []string{"alpha", "beta", "gamma", "junk"} {
		if err := s.Submit(id, contents[id]); err != nil {
			t.Fatal(err)
		}
	}
	churn()

	// Phase 2: delta re-sends alpha's content; its references miss the
	// junk-evicted cache and must be filled in automatically.
	if err := s.Submit("delta", contents["delta"]); err != nil {
		t.Fatal(err)
	}
	churn()

	needCount := 0
	for _, w := range history {
		dm, err := decodeMessage(w.body)
		if err != nil {
			t.Fatal(err)
		}
		if dm.typ == msgNeed {
			needCount++
		}
	}
	if needCount == 0 {
		t.Fatal("expected fill-in requests for delta")
	}

	for id, want := range contents {
		data, ok := got[id]
		if !ok {
			t.Fatalf("missing result for %s", id)
		}
		if !bytes.Equal(data, want) {
			t.Fatalf("result %s does not match original", id)
		}
	}
	if len(got) != len(contents) {
		t.Fatalf("unexpected extra results: %d vs %d", len(got), len(contents))
	}
	assertQuiet(t, s, r)

	// Phase 3: redeliver every old message (some duplicated) after the
	// originals were released. Nothing may change.
	rng.Shuffle(len(history), func(i, j int) { history[i], history[j] = history[j], history[i] })
	for _, w := range history {
		if w.toReceiver {
			if err := r.Deliver(w.body); err != nil {
				t.Fatal(err)
			}
			if rng.Intn(4) == 0 {
				r.Deliver(w.body)
			}
		} else {
			if err := s.Deliver(w.body); err != nil {
				t.Fatal(err)
			}
			if rng.Intn(4) == 0 {
				s.Deliver(w.body)
			}
		}
	}
	if res := r.TakeResults(); len(res) != 0 {
		t.Fatalf("late messages re-delivered %d results", len(res))
	}
	assertQuiet(t, s, r)
}
