# 实验室宽表 → 长表在线迁移 + 夜间清单对账

把旧采集程序的宽表 `samples(id, temperature, ph)` 在线迁移为按指标存放的长表
`sample_metrics(sample_id, metric, value)`，并提供 FastAPI 接口用于**发起迁移、查询进度、按样本/按指标查询**。
迁移期间旧程序可继续用原 SQL 增删改；切换是单事务原子完成的，切换后 `samples` 变成带
`INSTEAD OF` 触发器的兼容视图，旧 `SELECT / INSERT / UPDATE / DELETE` 无需改动即可继续工作。

同时提供**夜间清单对账 API**：接收分析仪清单（至多 5000 条互异样本 ID，`temperature`/`ph`
允许显式 `null`），与当次已提交的全部 samples 做快照比对，输出三类差异并保留两侧原值；
对账期间任何已提交写入都会使报告不可发布，发布后的写入只把旧报告标记为过期、内容仍可查。

## 快速开始（Docker Compose）

```bash
docker compose up --build -d

# 健康检查
curl -s localhost:8000/healthz

# 发起迁移（幂等）
curl -s -X POST localhost:8000/migrations

# 查询进度
curl -s localhost:8000/migrations/current

# 按样本查指标
curl -s localhost:8000/samples/1/metrics
# {"sample_id":1,"temperature":36.5,"ph":7.2}

# 按指标装载长表（keyset 分页）
curl -s 'localhost:8000/metrics/temperature?limit=5'

# 迁移期间/切换后用旧程序直接读写 samples
docker compose exec legacy-acquisition \
  python -m legacy.acquisition --db /app/data/lab.db select 1
docker compose exec legacy-acquisition \
  python -m legacy.acquisition --db /app/data/lab.db update 1 --temperature 37.0
docker compose exec legacy-acquisition \
  python -m legacy.acquisition --db /app/data/lab.db insert --temperature 36.6 --ph 7.1

# 提交夜间分析仪清单（显式 null 与字段缺失是两回事）
curl -s -X POST localhost:8000/reconciliations \
  -H 'content-type: application/json' \
  -d '{"entries":[
    {"sample_id":1,"temperature":36.5,"ph":7.2},
    {"sample_id":3,"temperature":null,"ph":7.05},
    {"sample_id":99001,"temperature":35.9}
  ]}'
# -> 202，拿到 report_id 后轮询状态
curl -s localhost:8000/reconciliations/<report_id>
# 按样本 ID 键集分页读取差异（可按 diff_type 过滤）
curl -s 'localhost:8000/reconciliations/<report_id>/diffs?limit=100&after_id=42'
```

API 文档：<http://localhost:8000/docs>。

演示脚本（需 API 已启动、数据库为初始宽表状态）：

```bash
python -m pip install -r requirements-dev.txt
PYTHONPATH=. python scripts/demo.py --db data/lab.db --api http://127.0.0.1:8000
PYTHONPATH=. python scripts/reconciliation_demo.py --api http://127.0.0.1:8000
```

本地非 Docker 启动：

```bash
python -m pip install -r requirements.txt
DATABASE_PATH=data/lab.db SEED_DATABASE_PATH=seed/lab_wide.db \
  uvicorn app.main:app --host 0.0.0.0 --port 8000
```

首次启动时，若数据库不存在，会把只读的示例库 `seed/lab_wide.db`（含 18 行宽表，
覆盖 NULL、id 空洞）复制到 `DATABASE_PATH`。示例库本身永不被修改。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/migrations` | 发起迁移；幂等。已完成 → `action=reused` 不重建；运行中 → 返回进度；失败 → 从检查点续跑 |
| GET | `/migrations/current` | 迁移状态与进度（`not_started / copying / completed / failed`） |
| GET | `/migrations/current/events` | 迁移事件日志 |
| GET | `/metrics/{metric}` | 按指标装载长表，`after_id` + `limit` 分页；`metric ∈ {temperature, ph}` |
| GET | `/samples/{sample_id}/metrics` | 按样本查指标（缺失返回 404） |
| GET | `/samples` | 兼容视图分页读取（等价旧程序 SELECT） |
| GET | `/samples/{sample_id}` | 兼容视图单行读取 |
| POST | `/reconciliations` | 提交分析仪清单发起对账（1–5000 条互异 ID），返回 `report_id`；202 |
| GET | `/reconciliations/{report_id}` | 报告状态、差异计数、来源、`stale` 过期标志 |
| GET | `/reconciliations/{report_id}/diffs` | 差异键集分页（`after_id`+`limit`，可按 `diff_type` 过滤），含两侧原值 |
| GET | `/healthz` | 健康检查 + 迁移状态 |

## 关键保证与实现

### 迁移期间旧程序可写，迟到搬迁不覆盖更正

- 宽表 `samples` 在迁移期间**仍是真实表**，旧程序原样读写。
- 启动迁移的同一个事务里装上三个 `AFTER INSERT/UPDATE/DELETE` 触发器，把每笔**已提交**的
  旧程序写入实时镜像进 `sample_metrics`（含 id 变更、删除、同号重建）。
- 搬迁按 `id` 顺序分块，每块只 `INSERT ... ON CONFLICT DO NOTHING`：**只补缺，不覆盖**。
  因此更晚执行的块不可能把触发器已写入的新值改回旧值；删除过的样本不会被重新拷回
  （分块时只读当时仍存在的 id）。
- 切换前最后一个 `BEGIN IMMEDIATE` 事务做终态追赶（以宽表为准 upsert）、删除已删样本的
  长表行、校验“每样本恰好 2 行”。

### 原子切换、不保留宽表副本

- 同一事务内：drop 镜像触发器 → `DROP TABLE samples` → 创建 `samples` 视图 + 三个
  `INSTEAD OF` 触发器。SQLite DDL 是事务性的，失败整体回滚，宽表原样保留。
- WAL 模式下读者只会看到“旧表”或“新视图”，不会看到中间态；每次 API 查询在单个读事务内完成。
- 切换后只有长表 `sample_metrics`，没有宽表副本；样本 id、数值、NULL 均保持不变，
  每个样本每项指标恰好一行（NULL 也有一行）。

### 切换后旧 SQL 继续有效

- `samples` 视图列名/顺序与旧表一致；`SELECT *`、按 id 等值/排序查询均可用。
- `INSERT`：显式重复 id 抛 `UNIQUE constraint failed: samples.id`；省略 id 时按原表语义分配
  `MAX(id)+1`；两指标行一次写入。
- `UPDATE`：改单个指标只覆盖该指标；`SET id=` 会整体搬移；写 NULL 会落成 NULL 行；
  `SET id = NULL` 与原表一样报 `datatype mismatch`，改成已存在的 id 报唯一约束错误。
- `DELETE`：删除该样本的两条指标行；同号重建正常。
- 旧程序提交的写入立刻出现在新 API 的查询里（两者读同一份已提交状态）。

> 说明：SQLite 不允许触发器修改 `last_insert_rowid()`。旧程序若 INSERT 时省略 id，
> 兼容层把分配的 id 记录在 `_insert_context`（单行辅助表）。仓库内的旧程序客户端
> `legacy/acquisition.py` 会读取它；第三方旧程序若依赖 `cursor.lastrowid`，请改用该表
> 或显式传 id。

## 夜间清单对账

### 三类差异、两侧原值、NULL 与缺失分开

- `manifest_only`：仅清单有（采集库漏行）；`database_only`：仅库中有（清单未覆盖）；
  `value_mismatch`：同号但至少一个**清单实际提供的**读数与库中不同。
- 每条差异同时保留 `manifest` 与 `database` 两侧原值。清单项带 `temperature_present` /
  `ph_present` 标志：**显式 JSON `null` 是一个值**（与库中 SQL NULL 相等、与非空值不等），
  字段**未提供**则该指标不参与比对、永不报 mismatch。
- 比对范围是提交时刻库中**全部已提交 samples**（宽表或兼容视图，报告 `source` 标明来源），
  而非只看清单里的 ID。

### 并发写入使报告不可发布，即使数值改回

- `db_revision` 单行计数器对逻辑 samples 数据集的**每次已提交写入**单调递增：迁移前由宽表上的
  `AFTER` 触发器递增，切换后由兼容视图的 `INSTEAD OF` 触发器递增（ABORT 回滚不会误增），
  切换事务本身也显式递增一次——跨越切换的对账因此必然中止。
- 作业提交时记录 `base_revision`；整个比对在**只读快照事务**里完成（WAL 下不阻塞任何写入），
  差异按块用短事务暂存。发布时在一个极短的写事务里复核修订号：
  不一致即标记 `aborted`，并在**同一事务**删除全部暂存行，失败不留半份结果。
- "数值改回原值"无法蒙混：修订号只增不减，只要期间有提交（含 UPDATE→UPDATE 改回、删除重建），
  报告就不发布，`failure_reason` 记录修订号变化。
- 对账全程不长占写锁：实测快照打开期间旧程序 INSERT 毫秒级完成；仅发布决策占用一次短写事务。

### 发布后过期不删、内容仍可分页读取

- 报告一旦 `published` 即冻结，其差异行永不删除；之后任何写入使当前修订号大于
  `published_revision` 时，读取惰性置 `stale=true`（同时持久化标志位），但报告内容照旧可查。
- 差异按 `sample_id` 键集分页（`after_id` + `limit`，`limit` 上限 1000），支持
  `diff_type` 过滤；`pending / aborted / failed` 的报告读不到任何差异行。
- 进程在作业进行中退出：重启时残留的 `pending` 作业被标记 `failed` 并清理暂存，
  不会出现永远卡住或半成品报告。

### 校验

- 清单 1–5000 条、`sample_id` 必须为互异整数（拒绝布尔/浮点/字符串）；
- 读数只接受 JSON number 或 `null`，拒绝布尔、字符串与 `NaN/Infinity`（返回 422，且错误响应不回显非法输入）。

### 中断重启、失败与幂等

- 进度 `last_id` 与数据块在同一事务提交；进程被杀后重启（`AUTO_RESUME=true`）从检查点续跑。
- 任意时刻崩溃：切换提交前的状态都保留宽表，旧程序仍可用；已提交数据不丢。
- 迁移失败（如磁盘错误）状态记为 `failed` 并保留错误；**不会自动重试**，避免崩溃循环，
  再次 `POST /migrations` 从检查点续跑。
- 已完成的迁移再次 `POST` 不会重建（提交时间、数据均不变）。
- 若切换事务已提交但进程在记录状态前崩溃，启动时会识别到 `samples` 已是视图，
  补记为 `completed`，不会重复迁移。

## 代码结构

```
app/config.py          环境变量配置
app/db.py              SQLite 连接 / 事务助手
app/schema.py          宽表、长表、迁移触发器、兼容视图、修订号与对账表
app/migration.py       在线分块复制 + 原子切换 + 断点续跑
app/repository.py      只读快照查询（按样本 / 按指标 / 兼容视图）
app/reconciliation.py  清单校验、快照比对、修订号并发检测、差异暂存与发布
app/main.py            FastAPI 路由
legacy/acquisition.py  旧采集程序客户端（原样 SQL，可 CLI 使用）
seed/lab_wide.db       示例宽表库（只读样例，含 NULL 与 id 空洞）
scripts/make_seed.py       重新生成示例库
scripts/demo.py            迁移端到端演示脚本
scripts/reconciliation_demo.py  对账端到端演示脚本
tests/                 单元 + API + 并发/中断 + 对账测试
```

## 测试

```bash
python -m pip install -r requirements-dev.txt
PYTHONPATH=. python -m pytest -q
```

覆盖：数值/NULL 保真、每样本每指标一行、旧程序切换后 CRUD 与重复 id、同号删除重建、
迁移中并发更正/删除/重建/新插入不被迟到块覆盖、失败重试、进程内中断续跑、
重复发起不重建、API 分页与校验、旧写入对 API 立即可见。

对账部分（`tests/test_reconciliation.py`，19 个用例）覆盖：三类差异分类与两侧原值、
显式 NULL 与字段缺失语义、键集分页与类型过滤、5000 条边界与各类非法清单 422、
并发写入中止发布（含改回原值）、中止/失败后暂存清理无半份结果、发布后过期仍可读、
快照期间不阻塞旧程序写入、迁移切换中对账中止、切换后兼容视图来源与修订号递增、
进程重启回收孤儿 pending 作业。

## 已知限制

- SQLite 是单写者：切换事务执行期间旧程序的写会短暂等待（默认 busy timeout 30s），
  不会失败；切换本身只做一次全表追赶，数据量极大时可考虑分批预拷贝（本实现已预拷贝）。
- 迁移进程内嵌在 API 进程，由单实例部署；多副本同时运行会让它们竞争同一状态行
  （有事务保护不会损坏，但不建议），如需水平扩展请把迁移放到独立 job 并保持单执行者。
- 兼容视图按 `GROUP BY` 读取；单样本等值查询走主键，大范围全表扫描较真实表慢，
  这是不保留宽表副本的取舍。
- 对账的并发检测基于库内修订号表：只有经由 `samples` 宽表/兼容视图的已提交写入会被计数
  （迁移引擎自身对 `sample_metrics` 的内部拷贝不经过 samples 触发器，但切换事务有显式递增）。
  若有外部程序绕过 `samples` 直接写 `sample_metrics`，修订号不会变化，本系统不支持这种写入路径。
- 对账作业运行在 API 进程内的守护线程，面向单实例部署；多副本共享同一 SQLite 文件时
  修订号仍正确（中止语义成立），但作业状态的线程内推进只发生在受理请求的那个副本。
- 已发布报告与其差异行持久保留，不做自动清理；长期运行需要按保留策略定期归档
  `reconciliation_runs / reconciliation_diffs`（当前未提供清理端点）。
- 未在此环境中实际执行 `docker compose up`（沙箱无 Docker daemon）：Compose/Dockerfile
  已完成静态校验，并用等价的本地 uvicorn 进程 + 同一份 `data` 卷路径做过端到端验证。
- 旧程序若在 `INSERT` 中使用 `INSERT OR IGNORE/REPLACE` 依赖冲突忽略/替换语义，
  兼容视图的 `RAISE(ABORT)` 会直接报错（原表也报错，行为一致但不会“忽略”）。
