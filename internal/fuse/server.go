package fuse

import (
	"sync"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// Server 封装 fuse.Server 提供生命周期管理。
// 业务侧通过 NewServer → Serve → Shutdown 使用，不直接接触 go-fuse Server。
type Server struct {
	inner      *fuse.Server
	mountPoint string
	wg         sync.WaitGroup
}

// NewServer 包装已挂载的 fuse.Server。
func NewServer(inner *fuse.Server, mountPoint string) *Server {
	return &Server{inner: inner, mountPoint: mountPoint}
}

// Serve 阻塞直到 Unmount 或出错。返回错误表示挂载异常终止。
func (s *Server) Serve() error {
	s.inner.Wait()
	return nil
}

// Shutdown 触发 Unmount，Serve 将返回。
func (s *Server) Shutdown() error {
	return s.inner.Unmount()
}

// MountPoint 返回当前挂载点。
func (s *Server) MountPoint() string {
	return s.mountPoint
}