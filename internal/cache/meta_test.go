package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"fuel/api"
	"fuel/internal/config"
)

func newTestMetaCache(statTTL, dirTTL, negTTL time.Duration) *metaCache {
	return NewMetaCache(config.MetaCacheConfig{
		StatTTL: statTTL,
		DirTTL:  dirTTL,
		NegTTL:  negTTL,
	}).(*metaCache)
}

func TestMetaCache_StatGetSet(t *testing.T) {
	c := newTestMetaCache(time.Second, time.Second, time.Second)
	defer stopMeta(c)

	entry := &api.MetaEntry{Path: "a/b", Size: 100, ETag: "etag1"}
	c.SetStat("a/b", entry)

	got, ok := c.GetStat("a/b")
	if !ok {
		t.Fatal("GetStat miss after SetStat")
	}
	if got.Size != 100 || got.ETag != "etag1" {
		t.Fatalf("GetStat = %+v, want size=100 etag=etag1", got)
	}

	got.Size = 999
	if again, _ := c.GetStat("a/b"); again.Size == 999 {
		t.Fatal("GetStat should return a copy, mutation leaked into cache")
	}
}

func TestMetaCache_StatMiss(t *testing.T) {
	c := newTestMetaCache(time.Second, time.Second, time.Second)
	defer stopMeta(c)

	if _, ok := c.GetStat("missing"); ok {
		t.Fatal("GetStat on missing path should miss")
	}
	if c.statMisses.Load() != 1 {
		t.Errorf("statMisses = %d, want 1", c.statMisses.Load())
	}
}

func TestMetaCache_StatDelete(t *testing.T) {
	c := newTestMetaCache(time.Second, time.Second, time.Second)
	defer stopMeta(c)

	c.SetStat("a", &api.MetaEntry{Path: "a"})
	c.DeleteStat("a")
	if _, ok := c.GetStat("a"); ok {
		t.Fatal("GetStat after DeleteStat should miss")
	}
}

func TestMetaCache_StatTTLExpiry(t *testing.T) {
	c := newTestMetaCache(50*time.Millisecond, time.Second, time.Second)
	defer stopMeta(c)

	c.SetStat("a", &api.MetaEntry{Path: "a"})
	time.Sleep(60 * time.Millisecond)
	if _, ok := c.GetStat("a"); ok {
		t.Fatal("GetStat after TTL expiry should miss")
	}
}

func TestMetaCache_SetStatClearsNeg(t *testing.T) {
	c := newTestMetaCache(time.Second, time.Second, time.Second)
	defer stopMeta(c)

	c.SetNeg("a")
	if !c.GetNeg("a") {
		t.Fatal("GetNeg should hit after SetNeg")
	}

	c.SetStat("a", &api.MetaEntry{Path: "a"})
	if c.GetNeg("a") {
		t.Fatal("SetStat should clear neg cache for same path")
	}
	if _, ok := c.GetStat("a"); !ok {
		t.Fatal("GetStat should hit after SetStat")
	}
}

func TestMetaCache_SetNegClearsStat(t *testing.T) {
	c := newTestMetaCache(time.Second, time.Second, time.Second)
	defer stopMeta(c)

	c.SetStat("a", &api.MetaEntry{Path: "a"})
	c.SetNeg("a")
	if _, ok := c.GetStat("a"); ok {
		t.Fatal("SetNeg should clear stat cache for same path")
	}
	if !c.GetNeg("a") {
		t.Fatal("GetNeg should hit after SetNeg")
	}
}

func TestMetaCache_DirGetSet(t *testing.T) {
	c := newTestMetaCache(time.Second, time.Second, time.Second)
	defer stopMeta(c)

	entries := []api.DirEntry{
		{Name: "file1", IsDir: false, Meta: &api.MetaEntry{Path: "dir/file1"}},
		{Name: "sub", IsDir: true, Meta: &api.MetaEntry{Path: "dir/sub", IsDir: true}},
	}
	c.SetDir("dir", entries)

	got, ok := c.GetDir("dir")
	if !ok {
		t.Fatal("GetDir miss after SetDir")
	}
	if len(got) != 2 {
		t.Fatalf("GetDir len = %d, want 2", len(got))
	}

	got[0].Name = "MUTATED"
	if again, _ := c.GetDir("dir"); again[0].Name == "MUTATED" {
		t.Fatal("GetDir should return a copy, mutation leaked into cache")
	}
}

func TestMetaCache_DirTTLExpiry(t *testing.T) {
	c := newTestMetaCache(time.Second, 50*time.Millisecond, time.Second)
	defer stopMeta(c)

	c.SetDir("dir", []api.DirEntry{{Name: "f"}})
	time.Sleep(60 * time.Millisecond)
	if _, ok := c.GetDir("dir"); ok {
		t.Fatal("GetDir after TTL expiry should miss")
	}
}

func TestMetaCache_NegTTLExpiry(t *testing.T) {
	c := newTestMetaCache(time.Second, time.Second, 50*time.Millisecond)
	defer stopMeta(c)

	c.SetNeg("a")
	time.Sleep(60 * time.Millisecond)
	if c.GetNeg("a") {
		t.Fatal("GetNeg after TTL expiry should miss")
	}
}

func TestMetaCache_InvalidatePrefix(t *testing.T) {
	c := newTestMetaCache(time.Second, time.Second, time.Second)
	defer stopMeta(c)

	c.SetStat("a/b/c", &api.MetaEntry{Path: "a/b/c"})
	c.SetStat("a/b/d", &api.MetaEntry{Path: "a/b/d"})
	c.SetStat("a/x/y", &api.MetaEntry{Path: "a/x/y"})
	c.SetDir("a/b", []api.DirEntry{{Name: "c"}})
	c.SetDir("a/x", []api.DirEntry{{Name: "y"}})
	c.SetNeg("a/b/missing")
	c.SetNeg("a/x/missing")

	c.InvalidatePrefix("a/b")

	if _, ok := c.GetStat("a/b/c"); ok {
		t.Error("a/b/c stat should be invalidated")
	}
	if _, ok := c.GetStat("a/b/d"); ok {
		t.Error("a/b/d stat should be invalidated")
	}
	if _, ok := c.GetStat("a/x/y"); !ok {
		t.Error("a/x/y stat should NOT be invalidated")
	}
	if _, ok := c.GetDir("a/b"); ok {
		t.Error("a/b dir should be invalidated")
	}
	if _, ok := c.GetDir("a/x"); !ok {
		t.Error("a/x dir should NOT be invalidated")
	}
	if c.GetNeg("a/b/missing") {
		t.Error("a/b/missing neg should be invalidated")
	}
	if !c.GetNeg("a/x/missing") {
		t.Error("a/x/missing neg should NOT be invalidated")
	}
}

func TestMetaCache_InvalidatePrefix_Root(t *testing.T) {
	c := newTestMetaCache(time.Second, time.Second, time.Second)
	defer stopMeta(c)

	c.SetStat("a", &api.MetaEntry{Path: "a"})
	c.SetStat("b", &api.MetaEntry{Path: "b"})
	c.SetDir("a", []api.DirEntry{})
	c.SetNeg("c")

	c.InvalidatePrefix("")

	if _, ok := c.GetStat("a"); ok {
		t.Error("a stat should be invalidated after root invalidate")
	}
	if _, ok := c.GetStat("b"); ok {
		t.Error("b stat should be invalidated after root invalidate")
	}
	if c.Len() != 0 {
		t.Error("all caches should be empty after root invalidate")
	}
}

func TestMetaCache_InvalidatePrefix_NormalizesSlashes(t *testing.T) {
	c := newTestMetaCache(time.Second, time.Second, time.Second)
	defer stopMeta(c)

	c.SetStat("a/b/c", &api.MetaEntry{Path: "a/b/c"})
	c.SetStat("a/b/d", &api.MetaEntry{Path: "a/b/d"})

	c.InvalidatePrefix("/a/b/")

	if _, ok := c.GetStat("a/b/c"); ok {
		t.Error("a/b/c stat should be invalidated despite leading/trailing slashes in prefix")
	}
	if _, ok := c.GetStat("a/b/d"); ok {
		t.Error("a/b/d stat should be invalidated")
	}
}

func TestMetaCache_Stats(t *testing.T) {
	c := newTestMetaCache(time.Second, time.Second, time.Second)
	defer stopMeta(c)

	c.SetStat("a", &api.MetaEntry{Path: "a"})
	c.GetStat("a")
	c.GetStat("missing")
	c.SetDir("d", []api.DirEntry{})
	c.GetDir("d")
	c.GetDir("missing2")
	c.SetNeg("n")
	c.GetNeg("n")
	c.GetNeg("missing3")

	s := c.Stats()
	if s.StatHits != 1 || s.StatMisses != 1 {
		t.Errorf("stat hits/misses = %d/%d, want 1/1", s.StatHits, s.StatMisses)
	}
	if s.DirHits != 1 || s.DirMisses != 1 {
		t.Errorf("dir hits/misses = %d/%d, want 1/1", s.DirHits, s.DirMisses)
	}
	if s.NegHits != 1 || s.NegMisses != 1 {
		t.Errorf("neg hits/misses = %d/%d, want 1/1", s.NegHits, s.NegMisses)
	}
}

func TestMetaCache_Concurrent(t *testing.T) {
	c := newTestMetaCache(time.Second, time.Second, time.Second)
	defer stopMeta(c)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			path := fmt.Sprintf("p/%d", i)
			c.SetStat(path, &api.MetaEntry{Path: path})
			c.GetStat(path)
			c.SetNeg(path)
			c.GetNeg(path)
			c.GetDir(path)
			c.InvalidatePrefix("p")
		}(i)
	}
	wg.Wait()
}

func TestMetaCache_ZeroTTL(t *testing.T) {
	c := newTestMetaCache(0, 0, 0)
	defer stopMeta(c)

	c.SetStat("a", &api.MetaEntry{Path: "a"})
	if _, ok := c.GetStat("a"); ok {
		t.Fatal("zero TTL stat should always miss")
	}

	c.SetDir("d", []api.DirEntry{})
	if _, ok := c.GetDir("d"); ok {
		t.Fatal("zero TTL dir should always miss")
	}

	c.SetNeg("n")
	if c.GetNeg("n") {
		t.Fatal("zero TTL neg should always miss")
	}
}

func stopMeta(c *metaCache) {
	if c.stat != nil {
		c.stat.Stop()
	}
	if c.dir != nil {
		c.dir.Stop()
	}
	if c.neg != nil {
		c.neg.Stop()
	}
}
