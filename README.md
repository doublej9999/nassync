# NAS Sync（ZIP 解包 + MAP 入库 + 监控面板）

一个用于 **NAS 目录自动监听** 的小工具：

- 监听 `A` 目录下新增/变更的 `.zip` 文件
- 校验并解压其中 `.MAP` 文件到 `B` 目录
- 提取 `LOT/WAFER` 信息写入 PostgreSQL
- 将已处理 ZIP 移动到同级 `BACKUP` 目录
- 提供 Web 监控页面查看任务状态与入库记录

## 1. 项目结构

```text
nassync/
├─ main.py                # 主程序（监听、处理、入库、Web 面板）
├─ ddl.sql                # PostgreSQL 建表脚本
├─ nassync.spec           # PyInstaller 打包配置
├─ A/                     # 监听源目录（示例）
├─ B/                     # MAP 目标目录（示例）
└─ logs/                  # 运行日志输出目录
```

## 2. 处理规则

### 2.1 目录规则

仅处理满足以下路径规则的 ZIP：

```text
{WATCH_DIR}/{TYPE}/WAFER_MAP/*.zip
```

- `TYPE`：业务类型（如 `BP`、`CD`、`FBP`）
- 含 `BACKUP` 的路径会被跳过
- 非 `.zip` 文件会被跳过

### 2.2 文件规则

- ZIP 内仅处理 `.MAP` 文件
- `.MAP` 文件名必须匹配：`XXXXXX-XX`（6 位 lot + 2 位 wafer）
- `.MAP` 文件名前缀必须与 ZIP 文件名前缀一致
  - 例如：`G39S14.zip` 中应包含 `G39S14-01.MAP`

### 2.3 成功后的动作

1. 抽取并复制 `.MAP` 到：`{TARGET_DIR}/{TYPE}/WAFER_MAP/`
2. 向 `zip_record` 写入去重记录（唯一键：`type + lot_id + wafer_id`）
3. 更新 `zip_task_status` 状态为 `SUCCESS`
4. 将 ZIP 移动到同目录 `BACKUP` 下

## 3. 数据库初始化

本项目使用 PostgreSQL，先执行：

```sql
-- 在目标库中执行
\i ddl.sql
```

或手动执行 `ddl.sql` 中的建表语句（包含 `zip_record` 与 `zip_task_status` 两张表）。

## 4. 环境依赖

推荐 Python 3.10+。

```bash
pip install -r requirements.txt
```

> 如果你使用源码中的 `psycopg2`（非 binary 包），请确保本机已安装 PostgreSQL 对应的编译依赖。

## 5. 配置说明

程序支持**外置 JSON 配置**，默认按以下优先级读取：

1. 环境变量 `NASSYNC_CONFIG` 指向的配置文件
2. 程序目录下 `config.json`
3. 若都不存在，则回退到内置默认值

建议做法：

1. 复制 `config.example.json` 为 `config.json`
2. 修改目录、数据库与端口配置
3. 重启程序生效

核心配置项：

- 目录配置：`WATCH_DIR`、`TARGET_DIR`
  - 支持本地路径（如 `D:\\A`）
  - 支持 NAS UNC 路径（如 `\\\\NAS01\\fab\\A`）
  - 支持 SMB URL（如 `smb://nas01/fab/A`，程序会自动转为 UNC）
  - 当 `WATCH_DIR` 为 NAS 路径时，监听器会自动切换为轮询模式（`PollingObserver`）
- 数据库配置：`DB_HOST`、`DB_PORT`、`DB_NAME`、`DB_SCHEMA`、`DB_USER`、`DB_PASSWORD`
  - 支持环境变量 `NASSYNC_DB_PASSWORD` 覆盖 `config.json` 中的 `DB_PASSWORD`
- 运行参数：
  - `FILE_STABLE_CHECK_TIMES` / `FILE_STABLE_CHECK_INTERVAL_SEC`
  - `PROCESS_RETRY_TIMES` / `PROCESS_RETRY_INTERVAL_SEC` / `PROCESS_RETRY_BACKOFF_MAX_SEC`
  - `TASK_QUEUE_MAX_SIZE` / `EVENT_DEDUP_WINDOW_SEC` / `DASHBOARD_CACHE_TTL_SEC`
  - `INITIAL_SCAN`（启动时是否扫描历史 ZIP）
- Web 面板：`WEB_HOST`、`WEB_PORT`
- 同步控制：`SYNC_TYPES`（填写 `["BP","CD"]` 等类型，留空或不配置时同步所有）

示例（本地目录）：

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
  "SYNC_TYPES": [
    "BP",
    "CD"
  ],
  "FILE_STABLE_CHECK_TIMES": 3,
  "FILE_STABLE_CHECK_INTERVAL_SEC": 2.0,
  "PROCESS_RETRY_TIMES": 3,
  "PROCESS_RETRY_INTERVAL_SEC": 3.0,
  "PROCESS_RETRY_BACKOFF_MAX_SEC": 30.0,
  "TASK_QUEUE_MAX_SIZE": 2000,
  "EVENT_DEDUP_WINDOW_SEC": 1.0,
  "DASHBOARD_CACHE_TTL_SEC": 2.0,
  "INITIAL_SCAN": true,
  "WEB_HOST": "0.0.0.0",
  "WEB_PORT": 8080
}

`SYNC_TYPES` 中只会同步列表内的目录，所有值会自动变成大写，省略该字段或提供空数组则同步全部类型。
```

示例（NAS 目录）：

```json
{
  "WATCH_DIR": "\\\\NAS01\\fab\\A",
  "TARGET_DIR": "smb://nas01/fab/B"
}
```

建议将数据库密码通过环境变量传入：

```bash
set NASSYNC_DB_PASSWORD=your_password
python main.py
```

## 6. 启动方式

### 6.1 直接运行

```bash
python main.py
```

启动后会同时运行：

- 文件监听服务（watchdog）
- Web 监控服务（默认 `http://0.0.0.0:8080/dashboard`）

### 6.2 一键打包（Windows + Linux）

项目根目录已提供 `build_all.ps1`，可一次性产出：

- `dist/windows/nassync.exe`
- `dist/linux/nassync`

执行命令：

```powershell
powershell -ExecutionPolicy Bypass -File .\build_all.ps1
```

说明：

- Windows 包：本机 Python 虚拟环境构建
- Linux 包：通过 Docker 镜像 `python:3.11-bullseye` 构建
- 脚本会自动复制 `config.example.json` 到对应产物目录的 `config.json`

可选参数：

```powershell
# 仅打 Windows
powershell -ExecutionPolicy Bypass -File .\build_all.ps1 -SkipLinux

# 仅打 Linux
powershell -ExecutionPolicy Bypass -File .\build_all.ps1 -SkipWindows
```

如果配置文件不在可执行文件同目录，可用环境变量指定：

```bash
set NASSYNC_CONFIG=D:\deploy\nassync\config.json
dist\windows\nassync.exe
```

## 7. Web 监控接口

- 页面：`/dashboard`
- 数据接口：`/api/dashboard`

接口支持分页与关键词筛选参数：

- 任务：`task_page`、`task_page_size`、`task_q`
- 记录：`record_page`、`record_page_size`、`record_q`

## 8. 日志与排错

- 日志目录：`logs/watcher.log`（按天滚动，保留 30 天）
- 常见失败原因：
  - 文件上传未完成导致“不稳定”
  - ZIP 与 MAP 前缀不一致
  - MAP 文件名格式不符合 `XXXXXX-XX`
  - 数据库连接失败

建议优先查看：

1. `logs/watcher.log`
2. `zip_task_status.error_msg`

## 9. 快速验证

1. 创建目录：`A/<TYPE>/WAFER_MAP`
2. 投放一个测试 ZIP（含合法命名 `.MAP` 文件）
3. 观察：
   - ZIP 被移动到 `A/<TYPE>/WAFER_MAP/BACKUP`
   - MAP 出现在 `B/<TYPE>/WAFER_MAP`
   - 数据库两张表有对应记录
   - `/dashboard` 展示任务状态

## 10. 自动化测试

已提供基础 `pytest` 用例，覆盖：

- ZIP 中 MAP 提取与目标目录写入
- 空 MAP ZIP 的失败判定
- 路径合法性校验与重试退避计算

运行方式：

```bash
pip install -r requirements.txt
pytest -q
```
