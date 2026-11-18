# 实时计算服务中心

面向实时数据分析场景，提供流式计算定义、窗口聚合、策略告警和状态管理的实时数据平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流式源端 | `stream_source` | 数据源配置 | 源端名称、消息队列、Topic名称 |
| 窗口配置 | `window_config` | 窗口策略 | 策略名称、窗口类型、窗口大小 |
| 计算算子 | `calc_operator` | 算子定义 | 算子名称、算子类型、输入字段 |
| 状态存储 | `state_store` | 状态配置 | 配置名称、后端类型、状态键模式 |
| 水位追踪 | `watermark_track` | 水位记录 | 水位名称、数据源、当前水位 |
| 告警策略 | `alert_policy` | 告警规则 | 规则名称、监控指标、触发条件 |
| 滑动指标 | `sliding_index` | 指标计算 | 指标名称、计算窗口、聚合函数 |
| 流关联 | `join_stream` | 关联配置 | 配置名称、左流源、右流源 |
| 结果落地 | `sink_output` | 输出配置 | 配置名称、输出类型、目标地址 |
| 补数任务 | `backfill_job` | 补数作业 | 作业名称、目标指标、补数范围 |
| CEP规则 | `cep_rule` | 事件序列 | 序列名称、事件类型、匹配顺序 |
| 吞吐管理 | `throughput_mgr` | 吞吐配置 | 配置名称、数据源、最大QPS |
| 检查点 | `checkpoint_mgr` | 检查点配置 | 配置名称、间隔时间、超时时长 |
| 故障恢复 | `restore_plan` | 恢复方案 | 方案名称、任务范围、恢复模式 |
| 指标血缘 | `metric_lineage` | 血缘记录 | 记录ID、指标名称、上游源端 |
| 迟到处理 | `late_handler` | 处理策略 | 策略名称、允许延迟、迟到动作 |
| 任务拓扑 | `task_topology` | 拓扑配置 | 拓扑名称、算子链、并行度 |
| 数据采样 | `data_sampler` | 采样策略 | 策略名称、采样方式、采样比例 |
| 调试追踪 | `debug_trace` | 调试会话 | 会话名称、目标算子、采样数据 |
| 聚合落库 | `aggregate_sink` | 聚合任务 | 任务名称、源指标集、聚合粒度 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 窗口落库恢复稳定性（结果落地 sink_output）

针对「写入失败整批重试、重复写成功分区」的问题，新增独立的窗口落库恢复引擎
`backend/app/services/landing_recovery.py`，把落库建模为
**发布(publication) → 窗口键(window_key) → 版本(version) → 完整性报告(report)**：

- 按源端批次计算窗口指标，每个批次形成一次发布，落地记录按 `(发布, 窗口键)` 唯一，
  一个窗口键在一次发布中只产生一条成功记录。
- 失败重试只补齐失败分组、原样复用已成功窗口键，绝不整批重写、绝不重复落库。
- 发布版本号在 sink 维度单调连续；逐项补齐沿用同版本，冻结整批重发才分配新版本。
- 落库完整性报告每次写入/重试后重新生成，只统计唯一窗口键，
  且与实际可消费记录使用同一版本（`consumable_matches_report`）。
- 序号3并发异常按幂等键只登记一次；发布级锁 + 状态守卫保证并发重试只恢复一次、保持幂等。
- 重试期间新结果到达可合并进同发布并按窗口键去重；中断写入预置失败分组以便续传；
  事务失败整批回滚不留半成品，恢复时用原始批次快照还原原结果。
- 落地数据写入 `_sink_*` 内部表，注册后不进入通用 CRUD、模块清单与运营概览统计。

### 恢复接口（前缀 `/api/sink-recovery`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/publish` | 按源端批次发布窗口指标，支持失败分区/中断/事务失败注入 |
| GET | `/publications` | 列出 sink 下全部发布及终态 |
| GET | `/records` | 可消费唯一窗口记录（冻结/中断/半成品不外露） |
| POST | `/retry` | 恢复路径A：锁定失败分组逐项补齐（同版本） |
| POST | `/republish` | 恢复路径B：冻结整批后重新发布（连续新版本） |
| POST | `/recover-transaction` | 事务失败后用原始快照恢复原结果 |
| GET | `/failures/{id}` | 失败分组锁定清单（健康发布返回空态） |
| GET | `/reports/{id}` | 同一发布不同终态的完整性报告版本序列 |
| GET | `/summary` | 去重分组数量与成功文件一致的落库汇总 |
| POST | `/demo/scenario3` | 一键演示序号3失败+中断+并发重试只恢复一次 |

### 前端页面

- `/sink-recovery`：结果落地恢复台，含「发布批次 / 唯一分组数据集 /
  失败分组与重试 / 落库完整性报告」四个标签页，失败、重试、中断、空态均可观测。
- `/sink-recovery/report/:publicationId`：序号3详情页，串联失败分组与完整性报告，
  说明异常原因并展示同一发布在不同终态下的连续报告版本。

领域：数据工程
子方向：数据分析与数据应用
