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

CREATE TABLE IF NOT EXISTS zip_task_status (
    id BIGSERIAL PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    zip_name VARCHAR(255) NOT NULL,
    zip_path VARCHAR(1000) NOT NULL,
    status VARCHAR(20) NOT NULL,
    error_msg VARCHAR(1000),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_zip_task_status_status CHECK (status IN ('PENDING', 'SUCCESS', 'FAILED')),
    CONSTRAINT uq_zip_task_status_zip_path UNIQUE (zip_path)
);
