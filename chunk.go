package velocache

import "crypto/sha256"

const (
	// ChunkSize is the fixed block size every transfer is split into.
	ChunkSize = 8 * 1024
	// MaxTransferSize is the largest accepted transfer payload (4 MiB).
	MaxTransferSize = 4 << 20
	// MaxInFlight is the maximum number of concurrent unacknowledged transfers.
	MaxInFlight = 32
	// CacheCapacity is the receiver-side reusable content cache size (1 MiB),
	// accounted in plaintext bytes.
	CacheCapacity = 1 << 20
)

// Hash identifies a chunk by content. Only byte-identical chunks share a Hash.
type Hash [32]byte

func hashChunk(b []byte) Hash { return Hash(sha256.Sum256(b)) }

// splitChunks slices data into ChunkSize blocks; the last block may be short.
func splitChunks(data []byte) [][]byte {
	if len(data) == 0 {
		return nil
	}
	n := (len(data) + ChunkSize - 1) / ChunkSize
	chunks := make([][]byte, n)
	for i := 0; i < n; i++ {
		start := i * ChunkSize
		end := start + ChunkSize
		if end > len(data) {
			end = len(data)
		}
		chunks[i] = data[start:end]
	}
	return chunks
}
