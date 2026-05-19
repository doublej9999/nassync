-- 用于已经在跑的旧库：把误建在 zip_record 上的唯一索引补齐到代码实际使用的 t_zip_record 上
-- 修复 ON CONFLICT (type, lot_id, wafer_id) 报 "there is no unique or exclusion constraint matching" 的错误
--
-- 用法：
--   psql -h <host> -U <user> -d <db> -f migrations/2026_05_fix_ddl_table_prefix.sql
--
-- 如果历史数据里 (type, lot_id, wafer_id) 有重复，CREATE UNIQUE INDEX 会失败，
-- 需要先用下面被注释掉的 DELETE 语句去重（保留 id 最大的一条），再重跑本脚本。

-- 可选：先去重（默认注释掉，确认必要再开启）
-- DELETE FROM t_zip_record a
-- USING t_zip_record b
-- WHERE a.id < b.id
--   AND a.type = b.type
--   AND a.lot_id = b.lot_id
--   AND a.wafer_id = b.wafer_id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_t_zip_record
ON t_zip_record (type, lot_id, wafer_id);

CREATE INDEX IF NOT EXISTS idx_t_zip_record_created_at_desc
ON t_zip_record (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_t_zip_record_type_created_at_desc
ON t_zip_record (type, created_at DESC);

-- 顺手清理旧的、错误前缀的对象（如果它们确实是误建出来的空表，可手动 DROP，这里默认不动）
-- DROP TABLE IF EXISTS zip_record;
-- DROP TABLE IF EXISTS zip_task_status;
-- DROP TABLE IF EXISTS map_path_config;
-- DROP TABLE IF EXISTS service_lease;
-- DROP TABLE IF EXISTS zip_task_queue;
