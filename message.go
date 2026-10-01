package velocache

import "encoding/binary"

// BlockSize is the fixed chunk size; the last block of a transfer may be shorter.
const BlockSize = 8 * 1024

// hashLen is the length of a content hash in bytes (SHA-256).
const hashLen = 32

// Message types carried on the wire between sender and receiver.
const (
	msgData byte = 1 // sender -> receiver: full block content
	msgRef  byte = 2 // sender -> receiver: short reference to a cached block
	msgNeed byte = 3 // receiver -> sender: request content for a missing reference
	msgDone byte = 4 // receiver -> sender: transfer fully restored
)

// msgData:  [type:1][id:8][idx:4][total:4][payload...]
// msgRef:   [type:1][id:8][idx:4][total:4][hash:32]
// msgNeed:  [type:1][id:8][idx:4][hash:32]
// msgDone:  [type:1][id:8]

type dataMsg struct {
	id      uint64
	idx     uint32
	total   uint32
	payload []byte
}

type refMsg struct {
	id    uint64
	idx   uint32
	total uint32
	hash  [hashLen]byte
}

type needMsg struct {
	id   uint64
	idx  uint32
	hash [hashLen]byte
}

func encodeData(m dataMsg) []byte {
	buf := make([]byte, 1+8+4+4+len(m.payload))
	buf[0] = msgData
	binary.BigEndian.PutUint64(buf[1:9], m.id)
	binary.BigEndian.PutUint32(buf[9:13], m.idx)
	binary.BigEndian.PutUint32(buf[13:17], m.total)
	copy(buf[17:], m.payload)
	return buf
}

func decodeData(buf []byte) dataMsg {
	return dataMsg{
		id:      binary.BigEndian.Uint64(buf[1:9]),
		idx:     binary.BigEndian.Uint32(buf[9:13]),
		total:   binary.BigEndian.Uint32(buf[13:17]),
		payload: buf[17:],
	}
}

func encodeRef(m refMsg) []byte {
	buf := make([]byte, 1+8+4+4+hashLen)
	buf[0] = msgRef
	binary.BigEndian.PutUint64(buf[1:9], m.id)
	binary.BigEndian.PutUint32(buf[9:13], m.idx)
	binary.BigEndian.PutUint32(buf[13:17], m.total)
	copy(buf[17:], m.hash[:])
	return buf
}

func decodeRef(buf []byte) refMsg {
	var m refMsg
	m.id = binary.BigEndian.Uint64(buf[1:9])
	m.idx = binary.BigEndian.Uint32(buf[9:13])
	m.total = binary.BigEndian.Uint32(buf[13:17])
	copy(m.hash[:], buf[17:17+hashLen])
	return m
}

func encodeNeed(m needMsg) []byte {
	buf := make([]byte, 1+8+4+hashLen)
	buf[0] = msgNeed
	binary.BigEndian.PutUint64(buf[1:9], m.id)
	binary.BigEndian.PutUint32(buf[9:13], m.idx)
	copy(buf[13:], m.hash[:])
	return buf
}

func decodeNeed(buf []byte) needMsg {
	var m needMsg
	m.id = binary.BigEndian.Uint64(buf[1:9])
	m.idx = binary.BigEndian.Uint32(buf[9:13])
	copy(m.hash[:], buf[13:13+hashLen])
	return m
}

func encodeDone(id uint64) []byte {
	buf := make([]byte, 1+8)
	buf[0] = msgDone
	binary.BigEndian.PutUint64(buf[1:9], id)
	return buf
}

func decodeDone(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf[1:9])
}
