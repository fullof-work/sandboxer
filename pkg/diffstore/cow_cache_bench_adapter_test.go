package diffstore

import "context"

func newWorkingSetCache() (workingSetCache, error) {
	cache, err := NewCache(DefaultCacheSize, DefaultMaxDirtySize)
	if err != nil {
		return workingSetCache{}, err
	}
	return workingSetCache{options: []DiskOption{WithCache(cache)}, drain: func() error { return cache.Drain(context.Background()) }, close: cache.Close, stats: func() map[string]float64 {
		s := cache.Stats()
		return map[string]float64{"cache-peak-B": float64(s.PeakUsed), "dirty-peak-B": float64(s.PeakDirty), "cache-used-B": float64(s.Used), "dirty-used-B": float64(s.DirtyUsed)}
	}}, nil
}
