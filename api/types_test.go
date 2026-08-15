package api

import (
	"testing"
	"time"
)

func TestInodeFromPath_Stable(t *testing.T) {
	path := "a/b/c.txt"
	i1 := InodeFromPath(path)
	i2 := InodeFromPath(path)
	if i1 != i2 {
		t.Errorf("inode not stable: %d != %d", i1, i2)
	}
}

func TestInodeFromPath_NonZero(t *testing.T) {
	if inode := InodeFromPath(""); inode == 0 {
		t.Error("inode must not be 0")
	}
	if inode := InodeFromPath("x"); inode == 0 {
		t.Error("inode must not be 0")
	}
}

func TestInodeFromPath_Distinct(t *testing.T) {
	if InodeFromPath("a") == InodeFromPath("b") {
		t.Error("different paths should map to different inodes")
	}
}

func TestMetaEntryFromObjectMeta(t *testing.T) {
	mtime := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	om := &ObjectMeta{
		Key:          "train/f1.jpg",
		Size:         2048,
		ETag:         "etag-1",
		LastModified: mtime,
		ContentType:  "image/jpeg",
	}
	m := MetaEntryFromObjectMeta(om, 1000, 1000)

	if m.Path != om.Key {
		t.Errorf("Path = %q, want %q", m.Path, om.Key)
	}
	if m.Inode != InodeFromPath(om.Key) {
		t.Errorf("Inode mismatch")
	}
	if m.Size != 2048 {
		t.Errorf("Size = %d, want 2048", m.Size)
	}
	if m.ETag != "etag-1" {
		t.Errorf("ETag = %q", m.ETag)
	}
	if m.Mode != 0o644 {
		t.Errorf("Mode = %#o, want 0644", m.Mode)
	}
	if m.Uid != 1000 || m.Gid != 1000 {
		t.Errorf("Uid/Gid = %d/%d", m.Uid, m.Gid)
	}
	if !m.MTime.Equal(mtime) {
		t.Errorf("MTime = %v, want %v", m.MTime, mtime)
	}
	if m.Nlink != 1 {
		t.Errorf("Nlink = %d, want 1", m.Nlink)
	}
	if m.IsDir {
		t.Error("IsDir should be false for file")
	}
	if m.ContentType != "image/jpeg" {
		t.Errorf("ContentType = %q", m.ContentType)
	}
}

func TestDirMetaEntry(t *testing.T) {
	d := DirMetaEntry("train/", 1000, 1000)
	if !d.IsDir {
		t.Error("IsDir should be true")
	}
	if d.Mode != 0o755 {
		t.Errorf("Mode = %#o, want 0755", d.Mode)
	}
	if d.Nlink != 2 {
		t.Errorf("Nlink = %d, want 2", d.Nlink)
	}
	if d.Size != 0 {
		t.Errorf("Size = %d, want 0", d.Size)
	}
	if d.Inode != InodeFromPath("train/") {
		t.Errorf("Inode mismatch")
	}
}
