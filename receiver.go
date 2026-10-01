package velocache

import (
	"crypto/sha256"
	"os"
	"path/filepath"
)

// Receiver restores transfers from sender messages, reusing blocks held in a
// fixed-size on-disk LRU cache, and queues completed results for pickup.
type Receiver struct {
	dir     string
	cache   *blockCache
	outbox  [][]byte
	live    map[uint64]*recvTransfer
	done    map[uint64]bool // retained metadata to suppress duplicate delivery
	results []recvResult
}

type recvTransfer struct {
	total   uint32
	blocks  map[uint32][]byte        // restored block contents by index
	need    map[uint32][hashLen]byte // indexes waiting on a missing reference
	touched map[[hashLen]byte]bool   // hashes already counted as used by this transfer
}

type recvResult struct {
	id   uint64
	data []byte
}

// NewReceiver creates a Receiver using dir as its private local directory.
// The reusable content cache is stored as files under dir.
func NewReceiver(dir string) (*Receiver, error) {
	cacheDir := filepath.Join(dir, "cache")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, err
	}
	return &Receiver{
		dir:   dir,
		cache: newBlockCache(cacheDir, cacheCapacity),
		live:  make(map[uint64]*recvTransfer),
		done:  make(map[uint64]bool),
	}, nil
}

// Deliver feeds an inbound message (from the sender) to the receiver.
func (r *Receiver) Deliver(msg []byte) {
	if len(msg) == 0 {
		return
	}
	switch msg[0] {
	case msgData:
		r.onData(decodeData(msg))
	case msgRef:
		r.onRef(decodeRef(msg))
	}
}

func (r *Receiver) transfer(id uint64, total uint32) *recvTransfer {
	t, ok := r.live[id]
	if !ok {
		t = &recvTransfer{
			total:   total,
			blocks:  make(map[uint32][]byte),
			need:    make(map[uint32][hashLen]byte),
			touched: make(map[[hashLen]byte]bool),
		}
		r.live[id] = t
	}
	return t
}

func (r *Receiver) onData(m dataMsg) {
	if r.done[m.id] {
		return // late or duplicate message for a finished transfer
	}
	t := r.transfer(m.id, m.total)
	if _, ok := t.blocks[m.idx]; ok {
		return // duplicate: do not refresh usage order
	}
	payload := append([]byte(nil), m.payload...)
	h := sha256.Sum256(payload)
	r.cache.put(h, payload)
	t.blocks[m.idx] = payload
	delete(t.need, m.idx)
	// The same content may be awaited at other indexes of this transfer.
	for idx, want := range t.need {
		if want == h {
			t.blocks[idx] = payload
			delete(t.need, idx)
		}
	}
	r.maybeFinish(m.id, t)
}

func (r *Receiver) onRef(m refMsg) {
	if r.done[m.id] {
		return
	}
	t := r.transfer(m.id, m.total)
	if _, ok := t.blocks[m.idx]; ok {
		return // duplicate: do not refresh usage order
	}
	if _, ok := t.need[m.idx]; ok {
		return // already awaiting repair for this index
	}
	var data []byte
	var ok bool
	if t.touched[m.hash] {
		// Already counted as used by this transfer: read without
		// refreshing the usage order again.
		data, ok = r.cache.peek(m.hash)
	} else {
		t.touched[m.hash] = true
		data, ok = r.cache.get(m.hash)
	}
	if ok {
		t.blocks[m.idx] = data
		r.maybeFinish(m.id, t)
		return
	}
	// Referenced content was evicted (or never cached): ask the sender to
	// repair it. The transfer waits only for this block.
	t.need[m.idx] = m.hash
	r.outbox = append(r.outbox, encodeNeed(needMsg{id: m.id, idx: m.idx, hash: m.hash}))
}

func (r *Receiver) maybeFinish(id uint64, t *recvTransfer) {
	if uint32(len(t.blocks)) != t.total || len(t.need) != 0 {
		return
	}
	var data []byte
	for i := uint32(0); i < t.total; i++ {
		data = append(data, t.blocks[i]...)
	}
	delete(r.live, id)
	r.done[id] = true
	r.results = append(r.results, recvResult{id: id, data: data})
	r.outbox = append(r.outbox, encodeDone(id))
}

// NextMessage returns the next outbound message, or false if none is pending.
func (r *Receiver) NextMessage() (msg []byte, ok bool) {
	if len(r.outbox) == 0 {
		return nil, false
	}
	msg = r.outbox[0]
	r.outbox = r.outbox[1:]
	return msg, true
}

// NextResult returns the next completed transfer, or false if none is ready.
// Each transfer id is delivered at most once.
func (r *Receiver) NextResult() (id uint64, data []byte, ok bool) {
	if len(r.results) == 0 {
		return 0, nil, false
	}
	res := r.results[0]
	r.results = r.results[1:]
	return res.id, res.data, true
}
