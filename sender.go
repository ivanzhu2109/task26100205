package velocache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Sender submits transfers and emits wire messages. In-flight originals are
// spooled under dir until the receiver acknowledges completion, then deleted.
type Sender struct {
	dir      string
	inflight map[string]*sendTransfer
	known    map[Hash]bool // chunks already sent as data at least once
	outbox   [][]byte
}

type sendTransfer struct {
	id     string
	dir    string // spool directory holding the original chunks
	hashes []Hash
	total  uint32
}

var (
	ErrTooManyTransfers = errors.New("velocache: too many in-flight transfers")
	ErrTransferTooLarge = errors.New("velocache: transfer exceeds 4 MiB")
	ErrEmptyTransfer    = errors.New("velocache: empty transfers are not supported")
	ErrDuplicateID      = errors.New("velocache: transfer id already in flight")
	ErrIDTooLong        = errors.New("velocache: transfer id exceeds 255 bytes")
)

// NewSender creates a sender using dir as its private local directory.
func NewSender(dir string) (*Sender, error) {
	if err := os.MkdirAll(filepath.Join(dir, "inflight"), 0o755); err != nil {
		return nil, err
	}
	return &Sender{
		dir:      dir,
		inflight: make(map[string]*sendTransfer),
		known:    make(map[Hash]bool),
	}, nil
}

// Submit registers a new transfer and queues its initial messages. Chunks the
// receiver was seen to be offered before are sent as short references; the
// rest go out as full data. The original is spooled to disk so missing
// references can be filled in later without resubmission.
func (s *Sender) Submit(id string, data []byte) error {
	if len(id) > 255 {
		return ErrIDTooLong
	}
	if len(data) == 0 {
		return ErrEmptyTransfer
	}
	if len(data) > MaxTransferSize {
		return ErrTransferTooLarge
	}
	if _, ok := s.inflight[id]; ok {
		return ErrDuplicateID
	}
	if len(s.inflight) >= MaxInFlight {
		return ErrTooManyTransfers
	}

	chunks := splitChunks(data)
	t := &sendTransfer{
		id:     id,
		dir:    filepath.Join(s.dir, "inflight", spoolName(id)),
		hashes: make([]Hash, len(chunks)),
		total:  uint32(len(chunks)),
	}
	if err := os.MkdirAll(t.dir, 0o755); err != nil {
		return err
	}
	for i, chunk := range chunks {
		h := hashChunk(chunk)
		t.hashes[i] = h
		if err := os.WriteFile(t.chunkPath(i), chunk, 0o644); err != nil {
			os.RemoveAll(t.dir)
			return err
		}
		if s.known[h] {
			s.outbox = append(s.outbox, message{typ: msgRef, id: id, total: t.total, index: uint32(i), hash: h}.encode())
		} else {
			s.known[h] = true
			s.outbox = append(s.outbox, message{typ: msgData, id: id, total: t.total, index: uint32(i), hash: h, payload: chunk}.encode())
		}
	}
	s.inflight[id] = t
	return nil
}

func spoolName(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:16])
}

func (t *sendTransfer) chunkPath(i int) string {
	return filepath.Join(t.dir, fmt.Sprintf("chunk_%06d", i))
}

// Pending drains and returns queued outbound messages.
func (s *Sender) Pending() [][]byte {
	out := s.outbox
	s.outbox = nil
	return out
}

// Deliver processes one inbound message from the receiver. Messages for
// unknown or already-completed transfers are ignored.
func (s *Sender) Deliver(msg []byte) error {
	m, err := decodeMessage(msg)
	if err != nil {
		return err
	}
	switch m.typ {
	case msgAck:
		if t, ok := s.inflight[m.id]; ok {
			os.RemoveAll(t.dir)
			delete(s.inflight, m.id)
		}
	case msgNeed:
		t, ok := s.inflight[m.id]
		if !ok {
			return nil // original already released; nothing to fill in
		}
		for _, ix := range m.indices {
			if ix >= t.total {
				continue
			}
			chunk, err := os.ReadFile(t.chunkPath(int(ix)))
			if err != nil {
				return err
			}
			s.outbox = append(s.outbox, message{
				typ: msgData, id: m.id, total: t.total, index: ix,
				hash: t.hashes[ix], payload: chunk,
			}.encode())
		}
	default:
		// Receivers never send data/ref messages; ignore.
	}
	return nil
}

// InFlight reports the number of unacknowledged transfers.
func (s *Sender) InFlight() int { return len(s.inflight) }
