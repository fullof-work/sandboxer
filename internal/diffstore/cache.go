package diffstore

import "container/list"

type cacheEntry struct {
	page    uint64
	data    []byte
	decoded *node
}
type pageCache struct {
	capacity int
	lru      *list.List
	entries  map[uint64]*list.Element
}

func newCache(n int) *pageCache { return &pageCache{n, list.New(), make(map[uint64]*list.Element, n)} }
func (c *pageCache) get(page uint64) ([]byte, bool) {
	e, ok := c.entries[page]
	if !ok {
		return nil, false
	}
	c.lru.MoveToFront(e)
	return e.Value.(cacheEntry).data, true
}
func (c *pageCache) put(page uint64, b []byte) {
	if e, ok := c.entries[page]; ok {
		e.Value = cacheEntry{page: page, data: b}
		c.lru.MoveToFront(e)
		return
	}
	e := c.lru.PushFront(cacheEntry{page: page, data: b})
	c.entries[page] = e
	if c.lru.Len() > c.capacity {
		e = c.lru.Back()
		delete(c.entries, e.Value.(cacheEntry).page)
		c.lru.Remove(e)
	}
}
func (c *pageCache) getNode(page uint64) (node, bool) {
	e, ok := c.entries[page]
	if !ok {
		return node{}, false
	}
	v := e.Value.(cacheEntry)
	if v.decoded == nil {
		return node{}, false
	}
	c.lru.MoveToFront(e)
	return *v.decoded, true
}
func (c *pageCache) putNode(page uint64, n node) {
	if e, ok := c.entries[page]; ok {
		v := e.Value.(cacheEntry)
		v.decoded = &n
		e.Value = v
	}
}
func (c *pageCache) remove(page uint64) {
	if e, ok := c.entries[page]; ok {
		delete(c.entries, page)
		c.lru.Remove(e)
	}
}
