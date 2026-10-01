package velocache

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Wire message types exchanged between sender and receiver.
type msgType byte

const (
	msgData msgType = 1 // sender -> receiver: chunk content
	msgRef  msgType = 2 // sender -> receiver: short reference to a cached chunk
	msgNeed msgType = 3 // receiver -> sender: request chunk content fill-in
	msgAck  msgType = 4 // receiver -> sender: transfer fully reconstructed
)

// message is the decoded form of any wire message.
type message struct {
	typ     msgType
	id      string
	total   uint32
	index   uint32
	hash    Hash
	payload []byte
	indices []uint32
}

var errBadMessage = errors.New("velocache: malformed message")

// encode serializes m into a compact binary form:
//
//	byte 0: type; byte 1: id length; then id bytes; then type-specific fields.
func (m message) encode() []byte {
	if len(m.id) > 255 {
		panic("velocache: transfer id too long")
	}
	buf := []byte{byte(m.typ), byte(len(m.id))}
	buf = append(buf, m.id...)
	switch m.typ {
	case msgData:
		var hdr [12]byte
		binary.BigEndian.PutUint32(hdr[0:4], m.total)
		binary.BigEndian.PutUint32(hdr[4:8], m.index)
		binary.BigEndian.PutUint32(hdr[8:12], uint32(len(m.payload)))
		buf = append(buf, hdr[:]...)
		buf = append(buf, m.hash[:]...)
		buf = append(buf, m.payload...)
	case msgRef:
		var hdr [8]byte
		binary.BigEndian.PutUint32(hdr[0:4], m.total)
		binary.BigEndian.PutUint32(hdr[4:8], m.index)
		buf = append(buf, hdr[:]...)
		buf = append(buf, m.hash[:]...)
	case msgNeed:
		var hdr [2]byte
		binary.BigEndian.PutUint16(hdr[0:2], uint16(len(m.indices)))
		buf = append(buf, hdr[:]...)
		var ib [4]byte
		for _, ix := range m.indices {
			binary.BigEndian.PutUint32(ib[0:4], ix)
			buf = append(buf, ib[:]...)
		}
	case msgAck:
		// no extra fields
	default:
		panic(fmt.Sprintf("velocache: unknown message type %d", m.typ))
	}
	return buf
}

func decodeMessage(b []byte) (message, error) {
	var m message
	if len(b) < 2 {
		return m, errBadMessage
	}
	m.typ = msgType(b[0])
	idLen := int(b[1])
	if len(b) < 2+idLen {
		return m, errBadMessage
	}
	m.id = string(b[2 : 2+idLen])
	rest := b[2+idLen:]
	switch m.typ {
	case msgData:
		if len(rest) < 12+32 {
			return m, errBadMessage
		}
		m.total = binary.BigEndian.Uint32(rest[0:4])
		m.index = binary.BigEndian.Uint32(rest[4:8])
		payloadLen := int(binary.BigEndian.Uint32(rest[8:12]))
		copy(m.hash[:], rest[12:44])
		if len(rest) != 44+payloadLen {
			return m, errBadMessage
		}
		m.payload = rest[44:]
	case msgRef:
		if len(rest) != 8+32 {
			return m, errBadMessage
		}
		m.total = binary.BigEndian.Uint32(rest[0:4])
		m.index = binary.BigEndian.Uint32(rest[4:8])
		copy(m.hash[:], rest[8:40])
	case msgNeed:
		if len(rest) < 2 {
			return m, errBadMessage
		}
		count := int(binary.BigEndian.Uint16(rest[0:2]))
		if len(rest) != 2+4*count {
			return m, errBadMessage
		}
		if count > 0 {
			m.indices = make([]uint32, count)
			for i := 0; i < count; i++ {
				m.indices[i] = binary.BigEndian.Uint32(rest[2+4*i : 6+4*i])
			}
		}
	case msgAck:
		if len(rest) != 0 {
			return m, errBadMessage
		}
	default:
		return m, errBadMessage
	}
	return m, nil
}
