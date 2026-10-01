package velocache

import (
	"crypto/sha256"
	"errors"
	"os"
)

// Sender submits outgoing transfers and produces messages for the receiver.
// It keeps the original content of unacknowledged transfers in memory and
// releases it once the receiver confirms completion.
type Sender struct {
	dir     string
	outbox  [][]byte
	known   map[[hashLen]byte]bool // hashes already sent as full content
	pending map[uint64]*senderTransfer
}

type senderTransfer struct {
	blocks [][]byte
	hashes [][hashLen]byte
}

// NewSender creates a Sender using dir as its private local directory.
func NewSender(dir string) (*Sender, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Sender{
		dir:     dir,
		known:   make(map[[hashLen]byte]bool),
		pending: make(map[uint64]*senderTransfer),
	}, nil
}

// Submit queues a new transfer with the given unique id.
// Blocks already sent as full content before are replaced by short references;
// the original is retained until the receiver confirms completion, so missing
// references are repaired automatically without resubmission.
func (s *Sender) Submit(id uint64, data []byte) error {
	if _, dup := s.pending[id]; dup {
		return errors.New("velocache: duplicate transfer id")
	}
	var blocks [][]byte
	for off := 0; off < len(data) || off == 0; off += BlockSize {
		end := off + BlockSize
		if end > len(data) {
			end = len(data)
		}
		blocks = append(blocks, data[off:end])
		if end == len(data) {
			break
		}
	}
	t := &senderTransfer{blocks: blocks, hashes: make([][hashLen]byte, len(blocks))}
	total := uint32(len(blocks))
	for i, b := range blocks {
		h := sha256.Sum256(b)
		t.hashes[i] = h
		if s.known[h] {
			s.outbox = append(s.outbox, encodeRef(refMsg{id: id, idx: uint32(i), total: total, hash: h}))
		} else {
			s.known[h] = true
			s.outbox = append(s.outbox, encodeData(dataMsg{id: id, idx: uint32(i), total: total, payload: b}))
		}
	}
	s.pending[id] = t
	return nil
}

// NextMessage returns the next outbound message, or false if none is pending.
func (s *Sender) NextMessage() (msg []byte, ok bool) {
	if len(s.outbox) == 0 {
		return nil, false
	}
	msg = s.outbox[0]
	s.outbox = s.outbox[1:]
	return msg, true
}

// Deliver feeds an inbound message (from the receiver) to the sender.
func (s *Sender) Deliver(msg []byte) {
	if len(msg) == 0 {
		return
	}
	switch msg[0] {
	case msgNeed:
		m := decodeNeed(msg)
		t, ok := s.pending[m.id]
		if !ok {
			return // already confirmed and released; nothing to do
		}
		if int(m.idx) >= len(t.blocks) || t.hashes[m.idx] != m.hash {
			return
		}
		s.outbox = append(s.outbox, encodeData(dataMsg{
			id:      m.id,
			idx:     m.idx,
			total:   uint32(len(t.blocks)),
			payload: t.blocks[m.idx],
		}))
	case msgDone:
		id := decodeDone(msg)
		delete(s.pending, id) // release the retained original
	}
}
