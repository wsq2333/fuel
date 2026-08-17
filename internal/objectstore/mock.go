package objectstore

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"fuel/api"
)

// mockStore 是 ObjectStore 的内存 Mock 实现，用于单元测试。
// 不依赖真实对象存储服务，行为与 ObjectStore 接口契约一致。
type mockStore struct {
	mu        sync.RWMutex
	bucket    string
	objects   map[string]*mockObject
	headCalls int
	getCalls  int
}

type mockObject struct {
	data        []byte
	etag        string
	contentType string
	lastModified time.Time
}

// NewMockStore 构造一个 Mock ObjectStore。
func NewMockStore(bucket string) api.ObjectStore {
	return &mockStore{bucket: bucket, objects: make(map[string]*mockObject)}
}

func (m *mockStore) Head(ctx context.Context, key string) (*api.ObjectMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.headCalls++
	obj, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("object %s: %w", key, syscall.ENOENT)
	}
	return &api.ObjectMeta{
		Key:          key,
		Size:         int64(len(obj.data)),
		ETag:         obj.etag,
		LastModified: obj.lastModified,
		ContentType:  obj.contentType,
	}, nil
}

func (m *mockStore) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCalls++
	obj, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("object %s: %w", key, syscall.ENOENT)
	}
	if offset < 0 || offset > int64(len(obj.data)) {
		return nil, fmt.Errorf("object %s: invalid offset %d", key, offset)
	}
	data := obj.data[offset:]
	if length > 0 && length < int64(len(data)) {
		data = data[:length]
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), data...))), nil
}

func (m *mockStore) Put(ctx context.Context, key string, r io.Reader, size int64) (*api.ObjectMeta, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read put body for %s: %w", key, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = &mockObject{
		data:         data,
		etag:         mockETag(key, data),
		lastModified: time.Now(),
	}
	return &api.ObjectMeta{
		Key:          key,
		Size:         int64(len(data)),
		ETag:         m.objects[key].etag,
		LastModified: m.objects[key].lastModified,
	}, nil
}

func (m *mockStore) List(ctx context.Context, prefix, delimiter string, maxKeys int) ([]api.ObjectEntry, []string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var entries []api.ObjectEntry
	prefixSet := map[string]struct{}{}
	for key, obj := range m.objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		rest := key[len(prefix):]
		if delimiter != "" {
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				prefixSet[prefix+rest[:idx+len(delimiter)]] = struct{}{}
				continue
			}
		}
		entries = append(entries, api.ObjectEntry{Key: key, Size: int64(len(obj.data))})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	prefixes := make([]string, 0, len(prefixSet))
	for p := range prefixSet {
		prefixes = append(prefixes, p)
	}
	sort.Strings(prefixes)

	if maxKeys > 0 && len(entries) > maxKeys {
		entries = entries[:maxKeys]
	}
	return entries, prefixes, nil
}

func (m *mockStore) Copy(ctx context.Context, srcKey, dstKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[srcKey]
	if !ok {
		return fmt.Errorf("object %s: %w", srcKey, syscall.ENOENT)
	}
	data := append([]byte(nil), obj.data...)
	m.objects[dstKey] = &mockObject{
		data:         data,
		etag:         mockETag(dstKey, data),
		contentType:  obj.contentType,
		lastModified: time.Now(),
	}
	return nil
}

func (m *mockStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *mockStore) Bucket() string {
	return m.bucket
}

// mockETag 生成内容相关的确定性 ETag（模拟 OSS 整文件上传的 ETag = 内容 MD5）。
func mockETag(key string, data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}
