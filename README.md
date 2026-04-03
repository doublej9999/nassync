# NAS Sync（ZIP 解包 + MAP 入库 + 监控面板）

一个用于 **NAS 目录自动监听** 的小工具，支持高可用单活接管与持久化任务队列：

- 监听 `WATCH_DIR` 下新增/变更文件（支持 `.zip`、`.tar` 等后缀过滤）
- 支持在页面维护 `map_path_config`（`WATCH_DIR`/`TARGET_DIR`/`SYNC_TYPES`/`FILE_SUFFIXES`/`LAST_SCAN`）
- ZIP 内校验 `.MAP` 文件名并提取 `LOT/WAFER`
- 将记录写入 PostgreSQL（`zip_record` / `zip_task_status`）
- 提供 Web 监控页面查看任务状态与入库记录
- 支持 **leader/standby 单活租约**（自动接管）
- 支持 **DB 持久化任务队列**（进程重启后可继续处理）

## 1. 项目结构

```text
nassync/
├─ main.py                # 启动入口
├─ ddl.sql                # PostgreSQL 建表脚本
├─ config.example.json    # 配置模板
├─ nassync/
│  ├─ app.py              # 主循环（租约、监听、Web）
│  ├─ workers.py          # 任务队列与工作线程
│  ├─ processor.py        # 文件处理主逻辑
│  ├─ db.py               # 数据库访问与 HA 能力
│  ├─ watcher.py          # 监听器与生命周期
│  └─ dashboard.py        # Web 面板与 API
└─ tests/                 # pytest 用例
```

## 2. 处理规则

### 2.1 目录规则

默认仅处理满足以下路径规则的 ZIP：

```text
{WATCH_DIR}/{TYPE}/WAFER_MAP/*.zip
```

同时支持通过 `map_path_config` 动态配置监听目录与目标目录。

- 路径包含 `BACKUP` 会跳过
- 命中 `map_path_config` 且未配置 `file_suffixes` 时，后缀不限
- 配置 `file_suffixes` 时，仅处理匹配后缀（如 `.zip,.tar`）

### 2.2 文件规则（按后缀区分）

- `.zip` 文件：
  - 仅处理 ZIP 内 `.MAP`
  - `.MAP` 文件名格式：`XXXXXX-XX`（6 位 lot + 2 位 wafer）
  - `.MAP` 前缀需与 ZIP 文件名前缀一致（可通过配置关闭）
- 非 `.zip` 文件（如 `.tar`）：
  - 仅搬运 + 备份，不写入 `zip_record`
  - 任务状态仍记录到 `zip_task_status`

### 2.3 成功动作

1. 复制源文件到 `BACKUP` 目录
2. 将文件搬运到目标目录
3. 若为 ZIP：提取 LOT/WAFER 并写入 `zip_record`
4. 更新 `zip_task_status`
5. 处理成功后推进 `map_path_config.last_scan`（按 `GREATEST` 推进）

## 3. 高可用能力

### 3.1 单活租约（Leader/Standby）

- 使用 `service_lease` 表实现单活
- 只有 `leader` 实例启动目录监听
- `standby` 实例持续续租竞争，leader 故障后自动接管
- `/healthz` 返回当前角色 `role=leader|standby`

### 3.2 持久化任务队列

- 使用 `zip_task_queue` 表持久化任务
- 事件入队先写 DB，再由 worker 认领执行
- 任务状态机：`PENDING/RUNNING/RETRYING/SUCCESS/FAILED/SKIPPED`
- 可重试任务使用 `next_retry_at` 延迟重试

### 3.3 健康检查与运行指标

`GET /healthz` 返回：

- `status` / `watcher` / `web` / `db`
- `role`
- `queue_depth`
- `retrying_count`
- `oldest_pending_sec`
- `db_connect_failures`

## 4. 数据库初始化

先执行：

```sql
\i ddl.sql
```

核心表：

- 业务表：`zip_record`、`zip_task_status`、`map_path_config`
- HA 表：`service_lease`、`zip_task_queue`

## 5. 环境依赖

推荐 Python 3.10+。

```bash
pip install -r requirements.txt
```

## 6. 配置说明

配置加载优先级：

1. 环境变量 `NASSYNC_CONFIG` 指定文件
2. 程序目录下 `config.json`
3. 内置默认值

建议：复制 `config.example.json` 为 `config.json` 后修改。

### 6.1 核心配置

- 目录：`WATCH_DIR`、`TARGET_DIR`
- 数据库：`DB_HOST`、`DB_PORT`、`DB_NAME`、`DB_SCHEMA`、`DB_USER`、`DB_PASSWORD`
- 运行：
  - `FILE_STABLE_CHECK_TIMES`
  - `PROCESS_RETRY_TIMES` / `PROCESS_RETRY_INTERVAL_SEC` / `PROCESS_RETRY_BACKOFF_MAX_SEC`
  - `TASK_QUEUE_MAX_SIZE` / `EVENT_DEDUP_WINDOW_SEC` / `DASHBOARD_CACHE_TTL_SEC`
  - `INITIAL_SCAN`
- Web：`WEB_HOST`、`WEB_PORT`

### 6.2 高可用配置（新增）

- `USE_PERSISTENT_QUEUE`：是否启用 DB 持久化任务队列（建议 `true`）
- `TASK_FETCH_BATCH_SIZE`：worker 每次从 DB 拉取任务数
- `TASK_LOCK_SEC`：任务认领锁超时时间（秒）
- `SERVICE_NAME`：租约服务名（同一服务实例需一致）
- `INSTANCE_ID`：实例 ID（留空则自动生成）
- `LEASE_DURATION_SEC`：租约时长（秒）
- `LEASE_RENEW_INTERVAL_SEC`：续租间隔（秒，必须小于租约时长）

### 6.3 配置示例

```json
{
  "WATCH_DIR": "D:\\A",
  "TARGET_DIR": "D:\\B",
  "DB_HOST": "127.0.0.1",
  "DB_PORT": 5432,
  "DB_NAME": "postgres",
  "DB_SCHEMA": "public",
  "DB_USER": "postgres",
  "DB_PASSWORD": "CHANGE_ME",
  "DB_TABLE": "zip_record",
  "DB_TASK_TABLE": "zip_task_status",
  "LOG_DIR": ".\\logs",
  "FILE_STABLE_CHECK_TIMES": 3,
  "FILE_STABLE_CHECK_INTERVAL_SEC": 2.0,
  "PROCESS_RETRY_TIMES": 3,
  "PROCESS_RETRY_INTERVAL_SEC": 3.0,
  "PROCESS_RETRY_BACKOFF_MAX_SEC": 30.0,
  "TASK_QUEUE_MAX_SIZE": 2000,
  "EVENT_DEDUP_WINDOW_SEC": 1.0,
  "DASHBOARD_CACHE_TTL_SEC": 2.0,
  "INITIAL_SCAN": true,
  "USE_PERSISTENT_QUEUE": true,
  "TASK_FETCH_BATCH_SIZE": 20,
  "TASK_LOCK_SEC": 120,
  "SERVICE_NAME": "nassync-watcher",
  "INSTANCE_ID": "",
  "LEASE_DURATION_SEC": 30,
  "LEASE_RENEW_INTERVAL_SEC": 10,
  "WEB_HOST": "0.0.0.0",
  "WEB_PORT": 8080
}
```

## 7. 启动方式

### 7.1 直接运行

```bash
python main.py
```

启动后会运行：

- 文件监听服务（仅 leader 实例）
- worker 处理线程
- Web 监控服务（默认 `http://0.0.0.0:8080/dashboard`）

### 7.2 双实例部署建议

- 两个实例使用同一套数据库
- `SERVICE_NAME` 保持一致
- `INSTANCE_ID` 设置为不同值（或留空自动生成）
- 同时启动后自动形成 leader/standby

## 8. Web 接口

- 页面：`/dashboard`（含“统计页面 / 监控页面 / 配置页面”三个页签）
- 数据接口：`/api/dashboard`
- 健康检查：`/healthz`（返回 watcher/web/db 综合状态与队列深度）
- 路径配置接口：
  - `GET /api/map-path-config`
  - `POST /api/map-path-config`
  - `POST /api/map-path-config/delete`
- 健康检查：`GET /healthz`

## 9. 日志与排错

- 日志目录：`logs/watcher.log`（按天滚动，保留 30 天）
- 常见故障：
  - 文件上传未完成（不稳定）
  - ZIP/MAP 前缀不一致
  - MAP 文件名格式错误
  - 数据库短暂不可用

`/api/dashboard` 额外返回：

- `runtime`：运行期指标（事件总数、去抖跳过数、队列丢弃数、任务成功/失败/重试数、平均/最大耗时、队列深度）
- `health`：服务健康快照（`status`、`watcher`、`web`、`db`、`db_error`）
建议优先看：

1. `logs/watcher.log`
2. `zip_task_status.error_msg`
3. `/healthz` 指标（尤其 `queue_depth`、`retrying_count`）

## 10. 自动化测试

```bash
pip install -r requirements.txt
pytest -q
```

当前基础测试覆盖：

- 监听去重与生命周期
- ZIP 处理与回滚
- 配置路径解析
- worker 重试与异常恢复
