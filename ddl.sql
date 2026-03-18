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