package velocache

import (
	"container/list"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
)

// chunkCache is the receiver-side reusable content cache. Chunk bytes live in
// files under dir; the LRU index lives in memory. Capacity is accounted in
// plaintext bytes and never exceeded.
type chunkCache struct {
	dir      string
	capacity int
	size     int
	ll       *list.List // front = most recently used
	items    map[Hash]*list.Element
}

type cacheEntry struct {
	hash Hash
	size int
}

func newChunkCache(dir string, capacity int) (*chunkCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	c := &chunkCache{
		dir:      dir,
		capacity: capacity,
		ll:       list.New(),
		items:    make(map[Hash]*list.Element),
	}
	// Reload chunks left over from a previous run, oldest first so that
	// recent files end up most-recently-used.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type loaded struct {
		hash    Hash
		size    int
		modTime int64
	}
	var files []loaded
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		raw, err := hex.DecodeString(e.Name())
		if err != nil || len(raw) != len(Hash{}) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		var h Hash
		copy(h[:], raw)
		files = append(files, loaded{hash: h, size: int(info.Size()), modTime: info.ModTime().UnixNano()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].modTime < files[j].modTime })
	for _, f := range files {
		c.size += f.size
		c.items[f.hash] = c.ll.PushFront(cacheEntry{hash: f.hash, size: f.size})
	}
	for c.size > c.capacity {
		c.evictOldest()
	}
	return c, nil
}

func (c *chunkCache) path(h Hash) string {
	return filepath.Join(c.dir, hex.EncodeToString(h[:]))
}

// has reports whether hash is currently cached.
func (c *chunkCache) has(h Hash) bool {
	_, ok := c.items[h]
	return ok
}

// touch marks hash as most recently used. No-op if absent.
func (c *chunkCache) touch(h Hash) {
	if el, ok := c.items[h]; ok {
		c.ll.MoveToFront(el)
	}
}

// get returns the cached plaintext for hash, or nil if absent.
func (c *chunkCache) get(h Hash) ([]byte, error) {
	if _, ok := c.items[h]; !ok {
		return nil, nil
	}
	data, err := os.ReadFile(c.path(h))
	if os.IsNotExist(err) {
		// Index and disk disagree; drop the entry and report a miss.
		c.remove(h)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

// insert stores data under hash, evicting least-recently-used chunks until
// the plaintext total fits within capacity. Inserting an already-cached hash
// only refreshes its recency.
func (c *chunkCache) insert(h Hash, data []byte) error {
	if _, ok := c.items[h]; ok {
		c.touch(h)
		return nil
	}
	for c.size+len(data) > c.capacity && c.ll.Len() > 0 {
		c.evictOldest()
	}
	if len(data) > c.capacity {
		return nil // chunk larger than the whole cache: never cached
	}
	if err := os.WriteFile(c.path(h), data, 0o644); err != nil {
		return err
	}
	c.size += len(data)
	c.items[h] = c.ll.PushFront(cacheEntry{hash: h, size: len(data)})
	return nil
}

func (c *chunkCache) evictOldest() {
	back := c.ll.Back()
	if back == nil {
		return
	}
	entry := back.Value.(cacheEntry)
	os.Remove(c.path(entry.hash))
	c.size -= entry.size
	delete(c.items, entry.hash)
	c.ll.Remove(back)
}

func (c *chunkCache) remove(h Hash) {
	if el, ok := c.items[h]; ok {
		entry := el.Value.(cacheEntry)
		os.Remove(c.path(h))
		c.size -= entry.size
		delete(c.items, h)
		c.ll.Remove(el)
	}
}

// count reports the number of cached chunks (for tests).
func (c *chunkCache) count() int { return len(c.items) }

// totalSize reports the plaintext bytes currently held (for tests).
func (c *chunkCache) totalSize() int { return c.size }
