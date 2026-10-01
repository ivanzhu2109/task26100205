package velocache

import (
	"bytes"
	"path/filepath"
)

// Result is a fully reconstructed transfer, delivered exactly once per ID.
type Result struct {
	ID   string
	Data []byte
}

// Receiver reconstructs transfers, keeping its reusable content cache in
// files under dir and session state in memory.
type Receiver struct {
	dir       string
	cache     *chunkCache
	transfers map[string]*recvTransfer
	completed []Result
	finished  map[string]bool // IDs already completed (dedup late messages)
	outbox    [][]byte
}

type recvTransfer struct {
	total     uint32
	chunks    [][]byte
	resolved  []bool
	remaining int
	requested []bool
	needDirty bool
	used      map[Hash]bool // chunks already counted as used by this transfer
}

// NewReceiver creates a receiver using dir as its private local directory.
// The reusable content cache lives in files under dir and holds at most
// CacheCapacity plaintext bytes.
func NewReceiver(dir string) (*Receiver, error) {
	return newReceiverWithCapacity(dir, CacheCapacity)
}

func newReceiverWithCapacity(dir string, capacity int) (*Receiver, error) {
	cache, err := newChunkCache(filepath.Join(dir, "cache"), capacity)
	if err != nil {
		return nil, err
	}
	return &Receiver{
		dir:       dir,
		cache:     cache,
		transfers: make(map[string]*recvTransfer),
		finished:  make(map[string]bool),
	}, nil
}

// Deliver processes one inbound message from the sender. Duplicate, reordered
// and late messages are tolerated; messages for completed transfers never
// re-trigger delivery.
func (r *Receiver) Deliver(msg []byte) error {
	m, err := decodeMessage(msg)
	if err != nil {
		return err
	}
	switch m.typ {
	case msgData:
		r.handleData(m)
	case msgRef:
		r.handleRef(m)
	default:
		// Senders never emit need/ack messages; ignore.
	}
	return nil
}

func (r *Receiver) transferFor(id string, total uint32) *recvTransfer {
	if r.finished[id] {
		return nil
	}
	if total == 0 || total > uint32((MaxTransferSize+ChunkSize-1)/ChunkSize) {
		return nil
	}
	t, ok := r.transfers[id]
	if !ok {
		t = &recvTransfer{
			total:     total,
			chunks:    make([][]byte, total),
			resolved:  make([]bool, total),
			remaining: int(total),
			requested: make([]bool, total),
			used:      make(map[Hash]bool),
		}
		r.transfers[id] = t
	}
	if t.total != total {
		return nil // conflicting metadata; ignore message
	}
	return t
}

func (r *Receiver) handleData(m message) {
	t := r.transferFor(m.id, m.total)
	if t == nil || m.index >= m.total || t.resolved[m.index] {
		return
	}
	h := hashChunk(m.payload)
	if h != m.hash {
		return // content does not match its declared hash
	}
	t.chunks[m.index] = m.payload
	t.resolved[m.index] = true
	t.remaining--
	r.cacheUse(t, h, m.payload)
	if t.remaining == 0 {
		r.finish(m.id, t)
	}
}

func (r *Receiver) handleRef(m message) {
	t := r.transferFor(m.id, m.total)
	if t == nil || m.index >= m.total || t.resolved[m.index] {
		return
	}
	if r.cache.has(m.hash) {
		payload, err := r.cache.get(m.hash)
		if err == nil && payload != nil {
			t.chunks[m.index] = payload
			t.resolved[m.index] = true
			t.remaining--
			r.cacheUse(t, m.hash, nil)
			if t.remaining == 0 {
				r.finish(m.id, t)
			}
			return
		}
	}
	// Cache miss: ask the sender to fill in the chunk (once per chunk).
	if !t.requested[m.index] {
		t.requested[m.index] = true
		t.needDirty = true
	}
}

// cacheUse records that chunk h participates in transfer t. The first use per
// transfer refreshes LRU recency; repeats within the same transfer do not.
func (r *Receiver) cacheUse(t *recvTransfer, h Hash, payload []byte) {
	if t.used[h] {
		if !r.cache.has(h) && payload != nil {
			r.cache.insert(h, payload) // re-cache evicted content, no extra use
		}
		return
	}
	t.used[h] = true
	if r.cache.has(h) {
		r.cache.touch(h)
	} else if payload != nil {
		r.cache.insert(h, payload)
	}
}

func (r *Receiver) finish(id string, t *recvTransfer) {
	var buf bytes.Buffer
	for _, c := range t.chunks {
		buf.Write(c)
	}
	r.completed = append(r.completed, Result{ID: id, Data: buf.Bytes()})
	r.finished[id] = true
	delete(r.transfers, id)
	r.outbox = append(r.outbox, message{typ: msgAck, id: id}.encode())
}

// Pending drains and returns queued outbound messages, flushing fill-in
// requests for chunks whose references missed the cache.
func (r *Receiver) Pending() [][]byte {
	for id, t := range r.transfers {
		if !t.needDirty {
			continue
		}
		var indices []uint32
		for i := range t.requested {
			if t.requested[i] && !t.resolved[i] {
				indices = append(indices, uint32(i))
			}
		}
		t.needDirty = false
		if len(indices) > 0 {
			r.outbox = append(r.outbox, message{typ: msgNeed, id: id, indices: indices}.encode())
		}
	}
	out := r.outbox
	r.outbox = nil
	return out
}

// TakeResults drains completed transfers not yet retrieved by the caller.
// Retrieved results are no longer held or used as cache content.
func (r *Receiver) TakeResults() []Result {
	out := r.completed
	r.completed = nil
	return out
}

// PendingCount reports the number of incomplete transfers (for tests).
func (r *Receiver) PendingCount() int { return len(r.transfers) }

// CacheChunks reports how many chunks the reusable cache currently holds
// (for tests).
func (r *Receiver) CacheChunks() int { return r.cache.count() }
