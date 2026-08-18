-- Fuel MySQL 元数据引擎建表语句 (PLAN §7.1)
--
-- 设计 (IMPL_DESIGN §4.2 模式 C):
--   fuel_meta     (path PK, size, etag, mtime, is_dir, content_type, updated_at)
--   fuel_dentries (dir_path, child_name, is_dir, size, etag, mtime)
--
-- 无过期：元数据不过期，写路径主动失效 (INV-1: 对象存储是真相来源, MySQL 只是加速层)。
-- 持久化：进程重启后元数据不丢失 (PLAN §7.1 验证标准)。

CREATE TABLE IF NOT EXISTS fuel_meta (
    bucket       VARCHAR(255)  NOT NULL,
    path         VARCHAR(1024) NOT NULL,
    size         BIGINT        NOT NULL DEFAULT 0,
    etag         VARCHAR(64)   NOT NULL DEFAULT '',
    mtime        DATETIME(6)   NOT NULL,
    is_dir       TINYINT(1)    NOT NULL DEFAULT 0,
    content_type VARCHAR(255)  NOT NULL DEFAULT '',
    mode         INT UNSIGNED  NOT NULL DEFAULT 420,  -- 0644
    uid          INT UNSIGNED  NOT NULL DEFAULT 0,
    gid          INT UNSIGNED  NOT NULL DEFAULT 0,
    updated_at   DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (bucket, path)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS fuel_dentries (
    bucket       VARCHAR(255)  NOT NULL,
    dir_path     VARCHAR(1024) NOT NULL,
    child_name   VARCHAR(255)  NOT NULL,
    is_dir       TINYINT(1)    NOT NULL DEFAULT 0,
    size         BIGINT        NOT NULL DEFAULT 0,
    etag         VARCHAR(64)   NOT NULL DEFAULT '',
    mtime        DATETIME(6)   NOT NULL,
    content_type VARCHAR(255)  NOT NULL DEFAULT '',
    updated_at   DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (bucket, dir_path, child_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 级联失效按前缀扫描的辅助索引 (Invalidate 用 LIKE 'path/%')
CREATE INDEX idx_meta_path_prefix      ON fuel_meta (bucket, path);
CREATE INDEX idx_dentries_dir_prefix   ON fuel_dentries (bucket, dir_path);