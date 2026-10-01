package velocache

import (
	"bytes"
	"fmt"
	"testing"
)

func chunkOf(b byte, n int) []byte { return bytes.Repeat([]byte{b}, n) }

func TestCacheInsertGetEvict(t *testing.T) {
	c, err := newChunkCache(t.TempDir(), 2*ChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	a := chunkOf('a', ChunkSize)
	b := chunkOf('b', ChunkSize)
	d := chunkOf('d', ChunkSize)
	ha, hb, hd := hashChunk(a), hashChunk(b), hashChunk(d)

	c.insert(ha, a)
	c.insert(hb, b)
	if c.count() != 2 || c.totalSize() != 2*ChunkSize {
		t.Fatalf("unexpected cache state: count=%d size=%d", c.count(), c.totalSize())
	}
	c.touch(ha) // a is now most recently used, b oldest
	c.insert(hd, d)
	if c.has(hb) {
		t.Fatal("expected b (LRU) to be evicted")
	}
	if !c.has(ha) || !c.has(hd) {
		t.Fatal("expected a and d to remain")
	}
	got, err := c.get(ha)
	if err != nil || !bytes.Equal(got, a) {
		t.Fatalf("get returned wrong bytes: %v", err)
	}
	if miss, _ := c.get(hb); miss != nil {
		t.Fatal("expected miss for evicted chunk")
	}
}

func TestCacheNeverExceedsCapacity(t *testing.T) {
	c, err := newChunkCache(t.TempDir(), 2*ChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("chunk-%06d", i))
		data = append(data, chunkOf(byte('0'+i), ChunkSize-len(data))...)
		if err := c.insert(hashChunk(data), data); err != nil {
			t.Fatal(err)
		}
		if c.totalSize() > 2*ChunkSize {
			t.Fatalf("capacity exceeded: %d", c.totalSize())
		}
	}
	if c.count() != 2 {
		t.Fatalf("expected 2 chunks, got %d", c.count())
	}
}

func TestCacheReloadFromDisk(t *testing.T) {
	dir := t.TempDir()
	c, err := newChunkCache(dir, CacheCapacity)
	if err != nil {
		t.Fatal(err)
	}
	a := chunkOf('x', 100)
	c.insert(hashChunk(a), a)

	c2, err := newChunkCache(dir, CacheCapacity)
	if err != nil {
		t.Fatal(err)
	}
	if !c2.has(hashChunk(a)) || c2.totalSize() != 100 {
		t.Fatalf("reload lost chunk: count=%d size=%d", c2.count(), c2.totalSize())
	}
}

func TestCacheOversizeChunkNotStored(t *testing.T) {
	c, err := newChunkCache(t.TempDir(), 16)
	if err != nil {
		t.Fatal(err)
	}
	big := chunkOf('z', 100)
	if err := c.insert(hashChunk(big), big); err != nil {
		t.Fatal(err)
	}
	if c.has(hashChunk(big)) || c.totalSize() != 0 {
		t.Fatal("oversize chunk must not be cached")
	}
}
