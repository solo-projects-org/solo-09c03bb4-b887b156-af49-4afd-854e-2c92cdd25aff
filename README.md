# 无人机航迹小文件合并服务

测绘无人机分批上报航迹 CSV，本服务把每批存成独立 Parquet 片段、SQLite 仅存元数据，
并支持按航次手动合并、活动文件数查询与整航次 CSV 流式导出。

## 技术栈与布局

- **FastAPI + Uvicorn**：HTTP API 与 Swagger（`/docs`）
- **PyArrow**：Parquet 读写，全部按 record batch 流式处理，不整航次载入内存
- **SQLite（WAL 模式）**：只存元数据（航次、片段、批次、合并租约、导出 pin、GC 队列）
- 两个 API 实例共享同一个数据目录，由 SQLite WAL + 单事务发布协调并发

```
data/
├── metadata.db                 # SQLite，仅元数据
├── fragments/<voyage>/<uuid>.parquet   # 上传批次片段（不可变）
├── merged/<voyage>/<merge_id>.parquet  # 合并产物（不可变）
└── tmp/<instance>-<incarnation>/       # 各实例化身私有的未发布残件
```

## 启动

### Docker Compose（双实例）

```bash
docker compose up --build
# 实例 A: http://localhost:8001  （Swagger: /docs）
# 实例 B: http://localhost:8002  （Swagger: /docs）
```

两个实例挂载同一个命名卷 `track-data`，任一端口都能服务全部请求。

### 本地直接运行

```bash
pip install -r requirements.txt
DATA_ROOT=./data INSTANCE_ID=dev uvicorn app.main:app --port 8000
```

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/voyages/{voyage_id}/batches` | 上传单航次 CSV（点号/UTC时刻/经度/纬度/高度，≤1 万行），每批一个片段 |
| GET  | `/voyages/{voyage_id}/active-file-count` | 活动（已发布）片段数 |
| GET  | `/voyages/{voyage_id}` | 航次状态与全部片段清单 |
| GET  | `/voyages` | 航次列表 |
| POST | `/voyages/{voyage_id}/merge` | 手动合并：开始时的活动片段原子替换为一个 |
| GET  | `/voyages/{voyage_id}/export` | 流式导出航次完整 CSV（固定导出开始时的快照） |
| GET  | `/admin/gc` | 立即执行一次旧片段回收 |

CSV 表头接受中文规范名，也接受 `point_id/utc_time/lon/lat/alt` 等英文别名。

## 关键设计

### 1. 上传：先登记写意图，再写不可变文件，最后事务发布

上传开始先在 SQLite 登记一条 **write intent**（归属本进程的实例化身），片段随后写到
该化身私有的 `tmp/<instance>-<incarnation>/`，fsync 后 `rename` 到最终路径，再在一个
SQLite 事务里插入片段/批次元数据、累加航次行数并把 intent 置为 `committed`。
rename 之后、事务提交之前崩溃会留下孤儿文件，但该次上传不会被确认：只要归属化身仍
存活（心跳续租中），任何实例的回收都不会碰它；化身死亡且租约过期后，残件由回收器
自动删除，客户端可安全重试。每批独立成文件，保留全部输入行。

### 2. 合并：单事务原子发布，冲突显式报错

1. 事务内快照**合并开始时**的活动片段（按 seq 排序），插入 `writing` 合并行（带归属
   化身）与对应 write intent；同一航次已有进行中的合并时直接 `409`（部分唯一索引兜底）。
2. 两层租约 + 心跳线程：合并租约（`merges.lease_expires`）与实例化身租约
   （`instances.lease_expires`）在流式合并期间定期续租。**另一实例启动恢复时，只要
   归属化身存活且租约未过期，就不会把该合并标记为失败，也不会删除其临时/产物文件**；
   实例被杀后心跳停止、租约过期，另一实例才可接管并回收残件。
3. PyArrow 按 batch 把源片段串流写入新 Parquet，并校验行数严格等于源片段之和。
4. 发布在**单个事务**内完成：先过发布围栏（化身仍存活、intent 仍为 active），再插入
   新 merged 片段（继承源片段最小 seq）、源片段置 `superseded`、入 GC 队列、合并行置
   `committed`、intent 置 `committed`。围栏失败返回 503，绝不登记已被回收的文件或
   覆盖更新结果。事务未提交则旧数据原样可用，不存在半成品。
5. 合并期间新到的批次 seq 更大、保持 active，不会被覆盖；它们排在合并片段之后，
   导出顺序与输入顺序一致。

### 3. 导出：pin 快照 + 流式 CSV

导出开始时在事务内 pin 住当时的活动片段集合，然后按片段/batch 流式产出 CSV。
慢下载期间合并、上传照常完成；GC 跳过任何被未释放 pin 引用的文件。pin 在流结束
（含客户端断开）时释放，并带有 30 分钟 TTL 兜底。

### 4. 崩溃恢复与 GC（多实例共享目录安全）

每个进程启动时在 `instances` 表注册一个唯一**化身**（incarnation，即使复用同一
`INSTANCE_ID` 也不同），并由后台心跳续租；优雅退出时立即标记 `dead`，被 SIGKILL 时
租约在 `INSTANCE_LEASE_SECONDS`（默认 30 秒）后过期。所有文件写入都先登记
`write_intents`，临时文件按化身隔离到 `tmp/<instance>-<incarnation>/`。

启动恢复（每个实例都会执行，且不会打断存活的对端）：

1. 注册本进程化身（复用同一 `INSTANCE_ID` 时先把旧的存活行标记为 dead）；
2. 回收器（启动时与后台 GC 循环都会跑）只处理**归属化身已死亡或租约过期**的
   write intent：删除其临时文件与已落盘未登记的片段，逐条以条件 UPDATE 认领，
   两个实例并发执行也不会重复处理；删除失败/进程被杀时，已认领但文件仍在的
   intent 会在下次运行时退回 active 续跑，直到残件清完；
3. 归属不明或已死亡化身的临时目录整体移除；升级前遗留的 `tmp/` 扁平文件按 mtime
   宽限（超过一个化身租约窗口）后清理；
4. 仅把**归属化身死亡、租约过期或无归属**的 `writing` 合并标记为 `failed`，存活
   实例正常续租的合并不受影响；
5. 孤儿扫描只作为兜底：扫描到的每个文件在独立 IMMEDIATE 事务内二次核对“是否已登记
   为片段或仍是活跃 intent 的目标”，因此扫描与登记交错时刚发布的片段不会被误删；
6. 标记文件已不存在的 GC 队列为完成，剩余条目由后台 GC 循环继续回收。

GC 仅删除：已 `superseded`、过宽限期（默认 5 分钟）、且无有效 pin 引用的文件。
删除动作以单文件为单位并立即更新队列，删到一半崩溃也不会重复/遗漏。恢复与 GC
期间其他航次的上传、导出照常完成（WAL 读写并发 + 导出 pin 保护）。

相关环境变量：`INSTANCE_LEASE_SECONDS`（化身租约，默认 30）、
`INSTANCE_HEARTBEAT_SECONDS`（心跳间隔，默认 5）、`UPLOAD_SLEEP_SECONDS`
（仅测试用，拉长 rename 到登记的窗口以验证交错安全，默认 0）。

## 测试

```bash
pip install -r requirements.txt pytest httpx
python -m pytest tests/ -q
```

- `tests/test_main_flow.py`：上传/查询/合并/导出/GC/并发冲突（FastAPI TestClient）
- `tests/test_integration.py`：真实 uvicorn 子进程，覆盖各崩溃点 SIGKILL 后重启恢复、
  双实例交错操作、跨实例合并冲突、慢下载期间合并上传与 GC
- `tests/test_recovery.py`：滚动重启场景——对端启动不打断在途上传/合并、落盘待登记
  片段在归属存活期间保留、扫描与登记交错不误删、死亡写入者残件由存活对端自动回收
- `tests/test_reaper_unit.py`：回收中断续跑与幂等、发布围栏拒绝已回收 intent、
  孤儿扫描事务内二次核对
- `tests/test_memory.py`：tracemalloc 验证合并/导出/写入的峰值内存不随行数线性增长

崩溃注入通过环境变量 `CRASH_HOOK`（可选 `CRASH_HOOK_AFTER`）开启，生产不设置即无影响。
测试环境通过 `INSTANCE_LEASE_SECONDS=3` 缩短租约，以便快速验证死亡化身回收。

## 已知边界

- 单批上限 1 万行（`MAX_ROWS_PER_BATCH` 可调）；上传 CSV 在请求内解析，单批内存
  受该上限约束。合并与导出均为流式，不受航次总行数限制。
- 容器内单 worker；水平扩展通过 Compose 的多实例完成（本仓库即双实例）。
- 本环境未安装 Docker，Compose 文件随仓库交付但未在本机实际 `docker compose up`。
