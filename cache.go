package velocache

import (
	"container/list"
	"encoding/hex"
	"os"
	"path/filepath"
)

// cacheCapacity is the fixed size of the receiver's reusable content cache,
// measured in raw content bytes.
const cacheCapacity = 1 << 20 // 1 MiB

// blockCache is an LRU cache of content blocks stored as files under dir.
// The index lives in memory; block contents live on disk.
type blockCache struct {
	dir   string
	cap   int
	used  int
	ll    *list.List // front = most recently used; element type *cacheEntry
	items map[[hashLen]byte]*list.Element
}

type cacheEntry struct {
	hash [hashLen]byte
	size int
}

func newBlockCache(dir string, capacity int) *blockCache {
	os.MkdirAll(dir, 0o755)
	return &blockCache{
		dir:   dir,
		cap:   capacity,
		ll:    list.New(),
		items: make(map[[hashLen]byte]*list.Element),
	}
}

func (c *blockCache) path(hash [hashLen]byte) string {
	return filepath.Join(c.dir, hex.EncodeToString(hash[:])+".blk")
}

// has reports whether hash is currently cached.
func (c *blockCache) has(hash [hashLen]byte) bool {
	_, ok := c.items[hash]
	return ok
}

// get returns the block content and marks it used. ok is false if absent.
func (c *blockCache) get(hash [hashLen]byte) (data []byte, ok bool) {
	el, ok := c.items[hash]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(el)
	data, err := os.ReadFile(c.path(hash))
	if err != nil {
		return nil, false
	}
	return data, true
}

// peek returns the block content without affecting usage order.
func (c *blockCache) peek(hash [hashLen]byte) (data []byte, ok bool) {
	if _, ok := c.items[hash]; !ok {
		return nil, false
	}
	data, err := os.ReadFile(c.path(hash))
	if err != nil {
		return nil, false
	}
	return data, true
}

// put inserts a block, evicting least-recently-used blocks as needed.
// Inserting an already-present hash does not refresh its usage.
func (c *blockCache) put(hash [hashLen]byte, data []byte) {
	if _, ok := c.items[hash]; ok {
		return
	}
	if len(data) > c.cap {
		return
	}
	for c.used+len(data) > c.cap && c.ll.Len() > 0 {
		back := c.ll.Back()
		entry := back.Value.(*cacheEntry)
		c.ll.Remove(back)
		delete(c.items, entry.hash)
		c.used -= entry.size
		os.Remove(c.path(entry.hash))
	}
	if err := os.WriteFile(c.path(hash), data, 0o644); err != nil {
		return
	}
	el := c.ll.PushFront(&cacheEntry{hash: hash, size: len(data)})
	c.items[hash] = el
	c.used += len(data)
}

// touch marks a cached block as used without reading its content.
func (c *blockCache) touch(hash [hashLen]byte) {
	if el, ok := c.items[hash]; ok {
		c.ll.MoveToFront(el)
	}
}
