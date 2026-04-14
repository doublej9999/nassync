CREATE TABLE IF NOT EXISTS zip_record (
    id BIGSERIAL PRIMARY KEY,
    type   VARCHAR(50) NOT NULL,
    lot_id    VARCHAR(100) NOT NULL,
    wafer_id    VARCHAR(50) NOT NULL,
    zip_name VARCHAR(255),
    zip_path VARCHAR(1000),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_zip_record
ON zip_record (type, lot_id, wafer_id);

CREATE INDEX IF NOT EXISTS idx_zip_record_created_at_desc
ON zip_record (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_zip_record_type_created_at_desc
ON zip_record (type, created_at DESC);

CREATE TABLE IF NOT EXISTS zip_task_status (
    id BIGSERIAL PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    zip_name VARCHAR(255) NOT NULL,
    zip_path VARCHAR(1000) NOT NULL,
    status VARCHAR(20) NOT NULL,
    error_msg VARCHAR(1000),
    is_feedback BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_zip_task_status_status CHECK (status IN ('PENDING', 'SUCCESS', 'FAILED')),
    CONSTRAINT uq_zip_task_status_zip_path UNIQUE (zip_path)
);

CREATE INDEX IF NOT EXISTS idx_zip_task_status_updated_at_desc
ON zip_task_status (updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_zip_task_status_type
ON zip_task_status (type, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_zip_task_status_is_feedback
ON zip_task_status (is_feedback, updated_at DESC);

ALTER TABLE zip_task_status
ADD COLUMN IF NOT EXISTS is_feedback BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS map_path_config (
    id BIGSERIAL PRIMARY KEY,
    sync_types VARCHAR(50) NOT NULL,
    watch_dir VARCHAR(1000) NOT NULL,
    target_dir VARCHAR(1000) NOT NULL,
    file_suffixes VARCHAR(500) NOT NULL DEFAULT '',
    last_scan DOUBLE PRECISION NOT NULL DEFAULT 0,
    is_feedback BOOLEAN NOT NULL DEFAULT FALSE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_map_path_config_watch UNIQUE (watch_dir)
);

ALTER TABLE map_path_config
ADD COLUMN IF NOT EXISTS is_feedback BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE map_path_config
ADD COLUMN IF NOT EXISTS file_suffixes VARCHAR(500) NOT NULL DEFAULT '';

ALTER TABLE map_path_config
ADD COLUMN IF NOT EXISTS last_scan DOUBLE PRECISION NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_map_path_config_enabled
ON map_path_config (enabled, sync_types);

CREATE TABLE IF NOT EXISTS service_lease (
    service_name VARCHAR(128) PRIMARY KEY,
    owner_id VARCHAR(255) NOT NULL,
    lease_token BIGINT NOT NULL DEFAULT 0,
    lease_until TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE service_lease
ADD COLUMN IF NOT EXISTS lease_token BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS zip_task_queue (
    id BIGSERIAL PRIMARY KEY,
    zip_path VARCHAR(1000) NOT NULL,
    status VARCHAR(20) NOT NULL,
    attempt INT NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMP NULL,
    locked_by VARCHAR(255),
    locked_until TIMESTAMP NULL,
    last_error VARCHAR(1000),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_zip_task_queue_zip_path UNIQUE (zip_path),
    CONSTRAINT ck_zip_task_queue_status CHECK (
        status IN ('PENDING', 'RUNNING', 'RETRYING', 'SUCCESS', 'FAILED', 'SKIPPED', 'DEAD')
    )
);

ALTER TABLE zip_task_queue
DROP CONSTRAINT IF EXISTS ck_zip_task_queue_status;

ALTER TABLE zip_task_queue
ADD CONSTRAINT ck_zip_task_queue_status CHECK (
    status IN ('PENDING', 'RUNNING', 'RETRYING', 'SUCCESS', 'FAILED', 'SKIPPED', 'DEAD')
);

CREATE INDEX IF NOT EXISTS idx_zip_task_queue_pick
ON zip_task_queue (status, next_retry_at, updated_at);

CREATE INDEX IF NOT EXISTS idx_zip_task_queue_locked_until
ON zip_task_queue (locked_until);
