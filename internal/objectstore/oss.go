package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"

	"fuel/api"
	"fuel/internal/config"
)

// ossClient 是 OSS 后端的 ObjectStore 实现。
type ossClient struct {
	client *oss.Client
	bucket *oss.Bucket
	bucketName string
}

// init 注册 OSS 后端到工厂 (INV-8)。
func init() {
	RegisterObjectStore("oss", newOSSClient)
}

// newOSSClient 构造 OSS 后端实例。
func newOSSClient(cfg *config.Config) (api.ObjectStore, error) {
	client, err := oss.New(cfg.Storage.OSS.Endpoint, cfg.Storage.AccessKey, cfg.Storage.AccessSecret)
	if err != nil {
		return nil, fmt.Errorf("create oss client: %w", err)
	}
	bucket, err := client.Bucket(cfg.Storage.Bucket)
	if err != nil {
		return nil, fmt.Errorf("get oss bucket %s: %w", cfg.Storage.Bucket, err)
	}
	return &ossClient{client: client, bucket: bucket, bucketName: cfg.Storage.Bucket}, nil
}

func (c *ossClient) Head(ctx context.Context, key string) (*api.ObjectMeta, error) {
	var header map[string][]string
	err := doWithRetry(ctx, func() error {
		h, err := c.bucket.GetObjectDetailedMeta(key)
		if err != nil {
			return mapError(key, err)
		}
		header = h
		return nil
	})
	if err != nil {
		return nil, err
	}
	return metaFromHeader(key, header)
}

func (c *ossClient) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	var options []oss.Option
	if length > 0 {
		options = append(options, oss.Range(offset, offset+length-1))
	} else if offset > 0 {
		options = append(options, oss.Range(offset, -1))
	}

	var body io.ReadCloser
	err := doWithRetry(ctx, func() error {
		rc, err := c.bucket.GetObject(key, options...)
		if err != nil {
			return mapError(key, err)
		}
		body = rc
		return nil
	})
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (c *ossClient) Put(ctx context.Context, key string, r io.Reader, size int64) (*api.ObjectMeta, error) {
	err := doWithRetry(ctx, func() error {
		if err := c.bucket.PutObject(key, r); err != nil {
			return mapError(key, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return c.Head(ctx, key)
}

func (c *ossClient) List(ctx context.Context, prefix, delimiter string, maxKeys int) ([]api.ObjectEntry, []string, error) {
	options := []oss.Option{oss.Prefix(prefix)}
	if delimiter != "" {
		options = append(options, oss.Delimiter(delimiter))
	}
	if maxKeys > 0 {
		options = append(options, oss.MaxKeys(maxKeys))
	}

	var result oss.ListObjectsResultV2
	err := doWithRetry(ctx, func() error {
		res, err := c.bucket.ListObjectsV2(options...)
		if err != nil {
			return mapError(prefix, err)
		}
		result = res
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	entries := make([]api.ObjectEntry, 0, len(result.Objects))
	for _, obj := range result.Objects {
		entries = append(entries, api.ObjectEntry{Key: obj.Key, Size: obj.Size})
	}
	return entries, result.CommonPrefixes, nil
}

func (c *ossClient) Copy(ctx context.Context, srcKey, dstKey string) error {
	return doWithRetry(ctx, func() error {
		if _, err := c.bucket.CopyObject(srcKey, dstKey); err != nil {
			return mapError(srcKey, err)
		}
		return nil
	})
}

func (c *ossClient) Delete(ctx context.Context, key string) error {
	return doWithRetry(ctx, func() error {
		if err := c.bucket.DeleteObject(key); err != nil {
			return mapError(key, err)
		}
		return nil
	})
}

func (c *ossClient) Bucket() string {
	return c.bucketName
}

// metaFromHeader 从 HEAD 响应头解析 ObjectMeta。
func metaFromHeader(key string, header map[string][]string) (*api.ObjectMeta, error) {
	om := &api.ObjectMeta{Key: key}

	if v := headerGet(header, oss.HTTPHeaderContentLength); v != "" {
		size, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse content-length %q for %s: %w", v, key, err)
		}
		om.Size = size
	}
	om.ETag = trimETag(headerGet(header, oss.HTTPHeaderEtag))
	om.ContentType = headerGet(header, oss.HTTPHeaderContentType)
	if v := headerGet(header, oss.HTTPHeaderLastModified); v != "" {
		if t, err := time.Parse(time.RFC1123, v); err == nil {
			om.LastModified = t
		}
	}
	return om, nil
}

// headerGet 大小写不敏感地读取 HTTP 响应头。
func headerGet(header map[string][]string, key string) string {
	for k, vals := range header {
		if strings.EqualFold(k, key) && len(vals) > 0 {
			return vals[0]
		}
	}
	return ""
}

// trimETag 去除 OSS ETag 首尾的引号。
func trimETag(etag string) string {
	return strings.Trim(etag, `"`)
}

// mapError 将 OSS SDK 错误映射为 POSIX errno (IMPL_DESIGN §9.3)。
// 404 → ENOENT, 403 → EACCES, 其余保留原始错误（重试已在上层处理）。
func mapError(key string, err error) error {
	if err == nil {
		return nil
	}
	var svcErr oss.ServiceError
	if errors.As(err, &svcErr) {
		switch svcErr.StatusCode {
		case 404:
			return fmt.Errorf("object %s: %w", key, syscall.ENOENT)
		case 403:
			return fmt.Errorf("object %s: %w", key, syscall.EACCES)
		}
	}
	return fmt.Errorf("object %s: %w", key, err)
}
