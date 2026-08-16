package cache

import (
	"sync"
	"testing"
	"time"
)

func TestTTLCache_GetSet(t *testing.T) {
	c := newTTLCache[string](time.Second)
	defer c.Stop()

	c.Set("k1", "v1")
	v, ok := c.Get("k1")
	if !ok || v != "v1" {
		t.Fatalf("Get(k1) = (%q, %v), want (v1, true)", v, ok)
	}

	if _, ok := c.Get("missing"); ok {
		t.Fatal("Get(missing) should miss")
	}
}

func TestTTLCache_TTLExpiry(t *testing.T) {
	c := newTTLCache[int](50 * time.Millisecond)
	defer c.Stop()

	c.Set("k", 42)
	if v, ok := c.Get("k"); !ok || v != 42 {
		t.Fatalf("immediate Get = (%d, %v), want (42, true)", v, ok)
	}

	time.Sleep(60 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatal("Get after expiry should miss")
	}

	if c.Len() != 0 {
		t.Fatalf("Len after lazy expiry = %d, want 0", c.Len())
	}
}

func TestTTLCache_Overwrite(t *testing.T) {
	c := newTTLCache[string](time.Second)
	defer c.Stop()

	c.Set("k", "old")
	c.Set("k", "new")
	v, ok := c.Get("k")
	if !ok || v != "new" {
		t.Fatalf("after overwrite Get = (%q, %v), want (new, true)", v, ok)
	}
	if c.Len() != 1 {
		t.Fatalf("Len after overwrite = %d, want 1", c.Len())
	}
}

func TestTTLCache_Delete(t *testing.T) {
	c := newTTLCache[string](time.Second)
	defer c.Stop()

	c.Set("k", "v")
	c.Delete("k")
	if _, ok := c.Get("k"); ok {
		t.Fatal("Get after Delete should miss")
	}
	c.Delete("nonexistent")
}

func TestTTLCache_InvalidatePrefix(t *testing.T) {
	c := newTTLCache[string](time.Second)
	defer c.Stop()

	c.Set("a/b/c", "1")
	c.Set("a/b/d", "2")
	c.Set("a/x/y", "3")
	c.Set("z/z", "4")

	removed := c.InvalidatePrefix("a/b")
	if removed != 2 {
		t.Fatalf("InvalidatePrefix(a/b) removed = %d, want 2", removed)
	}
	if _, ok := c.Get("a/b/c"); ok {
		t.Fatal("a/b/c should be invalidated")
	}
	if _, ok := c.Get("a/x/y"); !ok {
		t.Fatal("a/x/y should NOT be invalidated")
	}

	removed = c.InvalidatePrefix("")
	if removed != 2 {
		t.Fatalf("InvalidatePrefix('') removed = %d, want 2 (remaining)", removed)
	}
	if c.Len() != 0 {
		t.Fatalf("Len after clear-all = %d, want 0", c.Len())
	}
}

func TestTTLCache_InvalidatePrefix_NonDirPrefix(t *testing.T) {
	c := newTTLCache[string](time.Second)
	defer c.Stop()

	c.Set("abc/def", "1")
	c.Set("ab/def", "2")

	removed := c.InvalidatePrefix("ab")
	if removed != 1 {
		t.Fatalf("InvalidatePrefix(ab) removed = %d, want 1 (only ab/, not abc/)", removed)
	}
	if _, ok := c.Get("abc/def"); !ok {
		t.Fatal("abc/def should NOT be invalidated by prefix ab/")
	}
}

func TestTTLCache_Concurrent(t *testing.T) {
	c := newTTLCache[int](time.Second)
	defer c.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.Set("k", i)
			c.Get("k")
			c.Delete("k")
		}(i)
	}
	wg.Wait()
}

func TestTTLCache_Janitor(t *testing.T) {
	c := newTTLCache[int](20 * time.Millisecond)
	defer c.Stop()

	c.Set("a", 1)
	c.Set("b", 2)

	time.Sleep(300 * time.Millisecond)

	c.mu.RLock()
	n := len(c.items)
	c.mu.RUnlock()
	if n != 0 {
		t.Fatalf("after janitor sweep, items = %d, want 0", n)
	}
}

func TestTTLCache_ZeroTTL_NeverExpires(t *testing.T) {
	c := newTTLCache[string](0)
	defer c.Stop()

	c.Set("k", "v")
	time.Sleep(20 * time.Millisecond)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("zero TTL should never expire")
	}
}

func TestTTLCache_StopIdempotent(t *testing.T) {
	c := newTTLCache[string](time.Second)
	c.Stop()
	c.Stop()
}
