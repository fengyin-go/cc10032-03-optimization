"""窗口落库恢复引擎：按源端批次计算窗口指标并幂等落地。

设计目标（对应恢复稳定性需求）：

1. 按源端批次（source batch）计算窗口指标，每个批次形成一次「发布」(publication)。
2. 落地记录按 (发布, 窗口键) 唯一：一个窗口键在一次发布中只能产生一条成功记录；
   失败重试只从失败分组继续，绝不整批重写、绝不重复写已成功分区。
3. 发布拥有单调连续的版本号；重试不换版本，只有整批重新发布才分配新版本。
4. 落库完整性报告与实际可消费记录严格使用同一版本，每次写入/重试后重新生成，
   分组统计只计算唯一窗口键。
5. 序号3的并发异常按幂等键只登记一次；重试加发布级锁，重复重试请求返回同一结果。
6. 重试期间允许新结果到达：失败分组可逐项补齐（recover_in_place），
   也可冻结整批后重新发布（freeze_republish），两种路径都保持版本连续。
7. 事务型失败回滚半成品：接口永远不暴露可消费的半成品结果。

引擎只依赖标准库与内存 store，落地表注册为内部表，不进入通用 CRUD 与概览统计。
"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from app.store import store

# ---- 内部表名 ----
PUBLICATION_TABLE = "_sink_publications"
LANDING_TABLE = "_sink_landing_records"
REPORT_TABLE = "_sink_integrity_reports"

for _table in (PUBLICATION_TABLE, LANDING_TABLE, REPORT_TABLE):
    store.register_internal_table(_table)

# 发布终态
PUB_RUNNING = "写入中"
PUB_PARTIAL = "部分失败"
PUB_COMPLETED = "已完成"
PUB_INTERRUPTED = "已中断"
PUB_FROZEN = "已冻结"

# 落地分组状态
GRP_FAILED = "failed"
GRP_SUCCESS = "success"
GRP_PENDING = "pending"


@dataclass
class WindowMetric:
    """单个窗口键的窗口指标结果（按源端批次计算得到）。"""

    window_key: str
    aggregates: dict[str, float]
    row_count: int
    watermark: float
    start_ts: float = 0.0
    end_ts: float = 0.0
    partition: str = ""
    source_batch: str = ""

    def to_payload(self) -> dict[str, Any]:
        return {
            "window_key": self.window_key,
            "aggregates": dict(self.aggregates),
            "row_count": self.row_count,
            "watermark": self.watermark,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "partition": self.partition,
            "source_batch": self.source_batch,
        }


@dataclass
class PublishOutcome:
    """一次发布或重试的结果摘要。"""

    publication_id: str
    sink: str
    version: int
    mode: str
    total_groups: int
    success_groups: int
    failed_groups: int
    unique_groups: int
    report_seq: int
    terminal_state: str
    report: dict[str, Any]
    duplicate_skipped: int = 0
    note: str = ""


class LandingRecoveryEngine:
    """封装窗口指标落库、失败恢复与完整性报告的全部状态机逻辑。"""

    def __init__(self) -> None:
        # 发布级锁：同一发布的重试串行化，保证并发异常只恢复一次
        self._locks: dict[str, threading.Lock] = {}
        # sink 维度的版本推进锁，保证版本号连续
        self._version_locks: dict[str, threading.Lock] = {}
        # 可注入的写入判定函数：partition -> bool。默认全部成功。
        self._write_gates: dict[str, Callable[[str], bool]] = {}

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    def _pub_lock(self, publication_id: str) -> threading.Lock:
        lock = self._locks.get(publication_id)
        if lock is None:
            lock = threading.Lock()
            self._locks[publication_id] = lock
        return lock

    def _version_lock(self, sink: str) -> threading.Lock:
        lock = self._version_locks.get(sink)
        if lock is None:
            lock = threading.Lock()
            self._version_locks[sink] = lock
        return lock

    @staticmethod
    def _next_id() -> str:
        return uuid.uuid4().hex[:16]

    def set_write_gate(self, sink: str, gate: Callable[[str], bool]) -> None:
        """注入写入判定，用于模拟某个分区/窗口键写入失败（瞬态故障）。"""
        self._write_gates[sink] = gate

    def clear_write_gate(self, sink: str) -> None:
        self._write_gates.pop(sink, None)

    def _attempt_write(self, sink: str, partition: str) -> bool:
        gate = self._write_gates.get(sink)
        if gate is None:
            return True
        return bool(gate(partition))

    # ---- 发布 / 记录 / 报告 的只读访问 ----

    def get_publication(self, publication_id: str) -> dict[str, Any] | None:
        for row in store.rows(PUBLICATION_TABLE):
            if row["publication_id"] == publication_id:
                return row
        return None

    def list_publications(self, sink: str) -> list[dict[str, Any]]:
        return [
            row for row in store.rows(PUBLICATION_TABLE)
            if row.get("sink") == sink
        ]

    def _landing_rows(self, publication_id: str) -> list[dict[str, Any]]:
        return [
            row for row in store.rows(LANDING_TABLE)
            if row.get("publication_id") == publication_id
        ]

    def list_consumable_records(self, sink: str) -> list[dict[str, Any]]:
        """返回某 sink 当前所有可消费（成功、已发布版本）的唯一窗口记录。

        每个窗口键只保留版本最高的一条成功记录；冻结发布、半成品
        （running/interrupted 发布）中的记录不返回，接口不暴露半成品。
        """
        # 窗口键 -> 该键最高版本的成功记录
        latest_by_key: dict[str, dict[str, Any]] = {}
        for row in store.rows(LANDING_TABLE):
            if row.get("sink") != sink:
                continue
            if row.get("write_status") != GRP_SUCCESS:
                continue
            pub = self.get_publication(row["publication_id"])
            if pub is None:
                continue
            # 冻结发布与未完成发布里的记录不可消费
            if pub["status"] in (PUB_RUNNING, PUB_INTERRUPTED, PUB_FROZEN):
                continue
            key = row["window_key"]
            current = latest_by_key.get(key)
            if current is None or row["version"] > current["version"]:
                latest_by_key[key] = row
        return sorted(
            latest_by_key.values(), key=lambda r: (r["window_key"], r["version"])
        )

    # ------------------------------------------------------------------
    # 发布：按源端批次写入窗口指标
    # ------------------------------------------------------------------

    def publish_window_metrics(
        self,
        sink: str,
        metrics: list[WindowMetric],
        *,
        source_batch: str | None = None,
        fail_partitions: set[str] | None = None,
        interrupt_after: int | None = None,
        transaction_fail: bool = False,
    ) -> PublishOutcome:
        """把一批窗口指标发布到落地表。

        - fail_partitions: 本批次需要模拟写入失败的分区/窗口键集合（瞬态故障）。
        - interrupt_after: 写入 N 个分组后模拟中断；中断发布保持「已中断」终态，
          已写入分组保留，未写分组可在恢复时继续。
        - transaction_fail: 事务型失败——整批回滚，不留下任何半成品可消费记录，
          原始批次完整保留以便恢复原结果。
        """
        batch_id = source_batch or f"batch_{int(time.time() * 1000)}"
        publication_id = self._next_id()

        with self._version_lock(sink):
            version = self._allocate_version(sink)
            publication = {
                "publication_id": publication_id,
                "sink": sink,
                "source_batch": batch_id,
                "version": version,
                "status": PUB_RUNNING,
                "created_at": time.time(),
                "updated_at": time.time(),
                "retry_count": 0,
                "frozen": False,
                # 原始批次快照：事务失败 / 整批重发时从这里恢复，结果不丢失
                "batch_snapshot": [m.to_payload() for m in metrics],
                "error_reason": "",
                "history": [{"seq": 0, "event": "publish_start", "ts": time.time()}],
            }
            store.rows(PUBLICATION_TABLE).append(publication)

            # 事务型失败：整批回滚，只保留失败发布元数据与原始快照，不产生落地记录
            if transaction_fail:
                publication["status"] = PUB_INTERRUPTED
                publication["error_reason"] = "事务提交失败，整批回滚，无半成品落地"
                publication["history"].append(
                    {"seq": 1, "event": "transaction_rollback", "ts": time.time()}
                )
                self._register_publish_failure(
                    publication, "事务提交失败：批次已回滚，等待恢复",
                )
                report = self._build_report(publication, terminal="transaction_failed")
                return self._outcome(publication, report, mode="publish", note="rolled_back")

            fail_set = set(fail_partitions or set())
            written = 0
            interrupted = False
            for metric in metrics:
                partition = metric.partition or metric.window_key
                if not interrupted:
                    will_fail = (
                        partition in fail_set
                        or not self._attempt_write(sink, partition)
                    )
                    record = self._upsert_landing(
                        publication, metric, partition,
                        status=GRP_FAILED if will_fail else GRP_SUCCESS,
                    )
                    written += 1
                    if will_fail:
                        self._register_group_failure(publication, record)
                    # 模拟写入中断：后续分组全部以失败态预置，等待恢复时继续
                    if interrupt_after is not None and written >= interrupt_after:
                        interrupted = True
                        publication["status"] = PUB_INTERRUPTED
                        publication["error_reason"] = (
                            f"写入在第 {written} 个分组后中断，剩余分组待恢复"
                        )
                        publication["history"].append(
                            {"seq": len(publication["history"]),
                             "event": "write_interrupted", "ts": time.time()}
                        )
                else:
                    # 中断分组：预置失败行（非半成品成功记录），恢复从这里继续
                    record = self._upsert_landing(
                        publication, metric, partition, status=GRP_FAILED,
                    )
                    record["last_error"] = "写入中断，分组尚未落地"
                    self._register_group_failure(publication, record)

            self._refresh_publication_status(publication)
            report = self._build_report(publication)
            return self._outcome(publication, report, mode="publish")

    # ------------------------------------------------------------------
    # 恢复路径 A：锁定失败分组，逐项补齐（复用已成功窗口键）
    # ------------------------------------------------------------------

    def retry_failed_groups(
        self,
        publication_id: str,
        *,
        still_failing: set[str] | None = None,
        new_metrics: list[WindowMetric] | None = None,
    ) -> PublishOutcome:
        """从失败分组继续重试，已成功的窗口键原样复用、绝不重写。

        并发安全：同一发布同时只允许一个恢复流程，重复请求返回同一份结果，
        序号3的并发异常因此只恢复一次。
        """
        publication = self.get_publication(publication_id)
        if publication is None:
            raise KeyError(f"发布 {publication_id} 不存在")

        lock = self._pub_lock(publication_id)
        if not lock.acquire(blocking=False):
            # 已有恢复在进行：幂等返回当前最新报告，不重复恢复
            report = self._build_report(publication)
            return self._outcome(
                publication, report, mode="retry_concurrent_skip",
                note="恢复已在进行，重复请求已幂等忽略",
            )
        try:
            # 双重检查：恢复进行中，或失败分组已被前一个请求处理完，
            # 都视为重复的并发异常，幂等返回同版本报告，只恢复一次。
            if publication.get("recovering") or not self.failure_groups(publication_id):
                report = self._build_report(publication)
                return self._outcome(
                    publication, report, mode="retry_concurrent_skip",
                    note="失败分组已恢复或恢复进行中，重复请求已幂等忽略",
                )
            publication["recovering"] = True
            publication["retry_count"] += 1
            publication["status"] = PUB_RUNNING
            publication["updated_at"] = time.time()
            publication["history"].append({
                "seq": len(publication["history"]),
                "event": "retry_failed_groups_start",
                "ts": time.time(),
            })

            # 重试期间新结果到达：合并进同一发布（同版本），按窗口键去重
            duplicate_skipped = 0
            if new_metrics:
                existing_keys = {
                    r["window_key"] for r in self._landing_rows(publication_id)
                }
                for metric in new_metrics:
                    if metric.window_key in existing_keys:
                        duplicate_skipped += 1
                        continue
                    partition = metric.partition or metric.window_key
                    will_fail = not self._attempt_write(
                        publication["sink"], partition,
                    )
                    self._upsert_landing(
                        publication, metric, partition,
                        status=GRP_FAILED if will_fail else GRP_SUCCESS,
                    )
                    existing_keys.add(metric.window_key)

            failing = set(still_failing or set())
            recovered = 0
            for record in self._landing_rows(publication_id):
                if record["write_status"] != GRP_FAILED:
                    continue  # 已成功分组复用，不重写
                partition = record.get("partition", record["window_key"])
                if partition in failing or not self._attempt_write(
                    publication["sink"], partition,
                ):
                    record["last_error"] = "分区仍不可用，等待下一轮重试"
                    continue
                record["write_status"] = GRP_SUCCESS
                record["recovered_at"] = time.time()
                record["attempts"] = record.get("attempts", 1) + 1
                record.pop("last_error", None)
                recovered += 1
                self._resolve_group_failure(publication, record)

            publication["history"].append({
                "seq": len(publication["history"]),
                "event": f"retry_failed_groups_done(recovered={recovered})",
                "ts": time.time(),
            })
            self._refresh_publication_status(publication)
            report = self._build_report(publication)
            outcome = self._outcome(
                publication, report, mode="retry_in_place",
                duplicate_skipped=duplicate_skipped,
            )
            outcome.note = f"本轮补齐 {recovered} 个失败分组"
            return outcome
        finally:
            publication["recovering"] = False
            lock.release()

    # ------------------------------------------------------------------
    # 恢复路径 B：冻结整批后重新发布（保持连续版本号）
    # ------------------------------------------------------------------

    def freeze_and_republish(
        self,
        publication_id: str,
        *,
        fail_partitions: set[str] | None = None,
    ) -> PublishOutcome:
        """冻结失败批次，基于原始批次快照重新发布为一个连续新版本。

        旧发布冻结后不再可消费；新版本从完整快照重新落地，窗口键仍保持唯一。
        """
        old = self.get_publication(publication_id)
        if old is None:
            raise KeyError(f"发布 {publication_id} 不存在")

        with self._version_lock(old["sink"]):
            old["frozen"] = True
            old["status"] = PUB_FROZEN
            old["updated_at"] = time.time()
            old["history"].append({
                "seq": len(old["history"]),
                "event": "frozen_for_republish", "ts": time.time(),
            })
            self._build_report(old, terminal="frozen")

            snapshot = old["batch_snapshot"]
            metrics = [
                WindowMetric(
                    window_key=item["window_key"],
                    aggregates=item["aggregates"],
                    row_count=item["row_count"],
                    watermark=item["watermark"],
                    start_ts=item.get("start_ts", 0.0),
                    end_ts=item.get("end_ts", 0.0),
                    partition=item.get("partition", ""),
                    source_batch=item.get("source_batch", old["source_batch"]),
                )
                for item in snapshot
            ]
            new_publication_id = self._next_id()
            new_version = self._allocate_version(old["sink"])
            new_pub = {
                "publication_id": new_publication_id,
                "sink": old["sink"],
                "source_batch": old["source_batch"],
                "version": new_version,
                "status": PUB_RUNNING,
                "created_at": time.time(),
                "updated_at": time.time(),
                "retry_count": 0,
                "frozen": False,
                "republished_from": publication_id,
                "batch_snapshot": snapshot,
                "error_reason": "",
                "history": [
                    {"seq": 0, "event": "republish_start",
                     "from": publication_id, "ts": time.time()},
                ],
            }
            store.rows(PUBLICATION_TABLE).append(new_pub)

            fail_set = set(fail_partitions or set())
            for metric in metrics:
                partition = metric.partition or metric.window_key
                will_fail = partition in fail_set or not self._attempt_write(
                    old["sink"], partition,
                )
                record = self._upsert_landing(
                    new_pub, metric, partition,
                    status=GRP_FAILED if will_fail else GRP_SUCCESS,
                )
                if will_fail:
                    self._register_group_failure(new_pub, record)

            self._refresh_publication_status(new_pub)
            report = self._build_report(new_pub)
            return self._outcome(new_pub, report, mode="freeze_republish")

    # ------------------------------------------------------------------
    # 事务失败恢复：用原始快照重新落地，恢复原结果
    # ------------------------------------------------------------------

    def recover_transaction(self, publication_id: str) -> PublishOutcome:
        """事务回滚后的恢复：从原始批次快照重新发布，版本连续、窗口键唯一。"""
        publication = self.get_publication(publication_id)
        if publication is None:
            raise KeyError(f"发布 {publication_id} 不存在")

        lock = self._pub_lock(publication_id)
        if not lock.acquire(blocking=False):
            report = self._build_report(publication)
            return self._outcome(
                publication, report, mode="retry_concurrent_skip",
                note="事务恢复已在进行，重复请求已幂等忽略",
            )
        try:
            # 恢复进行中，或事务回滚发布已被前一个请求恢复（已有落地记录），
            # 重复的并发请求幂等返回，事务失败只恢复一次。
            already_restored = (
                publication["status"] != PUB_INTERRUPTED
                and len(self._landing_rows(publication_id)) > 0
            )
            if publication.get("recovering") or already_restored:
                report = self._build_report(publication)
                return self._outcome(
                    publication, report, mode="retry_concurrent_skip",
                    note="事务恢复已完成或进行中，重复请求已幂等忽略",
                )
            publication["recovering"] = True
            snapshot = publication["batch_snapshot"]
            metrics = [
                WindowMetric(
                    window_key=item["window_key"],
                    aggregates=item["aggregates"],
                    row_count=item["row_count"],
                    watermark=item["watermark"],
                    start_ts=item.get("start_ts", 0.0),
                    end_ts=item.get("end_ts", 0.0),
                    partition=item.get("partition", ""),
                    source_batch=item.get("source_batch", publication["source_batch"]),
                )
                for item in snapshot
            ]
            publication["retry_count"] += 1
            publication["history"].append({
                "seq": len(publication["history"]),
                "event": "transaction_recover_start", "ts": time.time(),
            })
            # 恢复沿用同一发布与版本：清空回滚残留（事务失败本就无落地记录）
            for row in self._landing_rows(publication_id):
                store.rows(LANDING_TABLE).remove(row)
            for metric in metrics:
                partition = metric.partition or metric.window_key
                # 恢复时写入故障已清除：默认成功；仍受注入 gate 控制
                will_fail = not self._attempt_write(publication["sink"], partition)
                record = self._upsert_landing(
                    publication, metric, partition,
                    status=GRP_FAILED if will_fail else GRP_SUCCESS,
                )
                if will_fail:
                    self._register_group_failure(publication, record)
            publication["error_reason"] = ""
            self._refresh_publication_status(publication)
            report = self._build_report(publication, terminal="recovered")
            self._resolve_publish_failure(publication)
            return self._outcome(publication, report, mode="transaction_recover")
        finally:
            publication["recovering"] = False
            lock.release()

    # ------------------------------------------------------------------
    # 完整性报告：与可消费记录同版本，只统计唯一窗口键
    # ------------------------------------------------------------------

    def get_latest_report(self, sink: str) -> dict[str, Any] | None:
        reports = [
            r for r in store.rows(REPORT_TABLE) if r.get("sink") == sink
        ]
        if not reports:
            return None
        return max(reports, key=lambda r: (r["version"], r["report_seq"]))

    def list_reports(self, publication_id: str) -> list[dict[str, Any]]:
        return [
            r for r in store.rows(REPORT_TABLE)
            if r.get("publication_id") == publication_id
        ]

    def _build_report(
        self, publication: dict[str, Any], terminal: str | None = None,
    ) -> dict[str, Any]:
        """重新生成落库完整性报告。

        报告版本 = 发布版本，与该版本可消费记录严格一致；
        分组计数基于窗口键去重，成功文件数 = 唯一成功分组数。
        """
        sink = publication["sink"]
        rows = self._landing_rows(publication["publication_id"])
        unique_keys = {r["window_key"] for r in rows}
        success_keys = {
            r["window_key"] for r in rows if r["write_status"] == GRP_SUCCESS
        }
        failed_keys = {
            r["window_key"] for r in rows if r["write_status"] == GRP_FAILED
        }
        expected_keys = {item["window_key"] for item in publication["batch_snapshot"]}

        # 同 sink 内被冻结/被更新版本替代的窗口键不计入当前可消费集合
        consumable = self.list_consumable_records(sink)
        consumable_keys = {r["window_key"] for r in consumable}
        # 本版本成功分组必须全部出现在同版本可消费集合中（报告与消费同版本）
        same_version_consumable = {
            r["window_key"]
            for r in consumable
            if r["version"] == publication["version"]
        }
        version_aligned = success_keys.issubset(same_version_consumable)

        terminal_state = terminal or self._terminal_state(publication)
        report_seq = (
            max(
                (r["report_seq"] for r in store.rows(REPORT_TABLE)
                 if r.get("publication_id") == publication["publication_id"]),
                default=0,
            ) + 1
        )
        report = {
            "report_id": self._next_id(),
            "publication_id": publication["publication_id"],
            "sink": sink,
            "version": publication["version"],
            "report_seq": report_seq,
            "source_batch": publication["source_batch"],
            "terminal_state": terminal_state,
            "generated_at": time.time(),
            # 唯一分组口径
            "expected_groups": len(expected_keys),
            "unique_groups": len(unique_keys),
            "success_groups": len(success_keys),
            "failed_groups": len(failed_keys),
            "missing_groups": sorted(expected_keys - unique_keys),
            "duplicate_records": len(rows) - len(unique_keys),
            "success_files": len(success_keys),
            # 与可消费记录同版本核对：本版本成功分组必须能以同版本被消费
            "consumable_version": publication["version"],
            "consumable_unique_groups": len(consumable_keys),
            "consumable_matches_report": version_aligned,
            "integrity_rate": len(success_keys) / max(len(expected_keys), 1),
            "complete": len(failed_keys) == 0
            and expected_keys.issubset(success_keys),
            "retry_count": publication["retry_count"],
            "error_reason": publication.get("error_reason", ""),
        }
        store.rows(REPORT_TABLE).append(report)
        return report

    # ------------------------------------------------------------------
    # 状态与版本管理
    # ------------------------------------------------------------------

    def _allocate_version(self, sink: str) -> int:
        """分配 sink 维度单调连续版本号（调用方持有版本锁）。"""
        existing = [p["version"] for p in self.list_publications(sink)]
        return (max(existing) + 1) if existing else 1

    def _refresh_publication_status(self, publication: dict[str, Any]) -> None:
        if publication["status"] == PUB_FROZEN:
            return
        # 中断态在仍有失败/缺失分组时保留；全部补齐后才转为正常终态
        was_interrupted = publication["status"] == PUB_INTERRUPTED
        rows = self._landing_rows(publication["publication_id"])
        has_failed = any(r["write_status"] == GRP_FAILED for r in rows)
        expected = len(publication["batch_snapshot"])
        written = len(rows)
        if not has_failed and written >= expected:
            publication["status"] = PUB_COMPLETED
            publication["error_reason"] = ""
            self._resolve_publish_failure(publication)
        elif was_interrupted:
            # 中断后仍有分组未成功：保持中断终态，等待页面触发恢复
            publication["status"] = PUB_INTERRUPTED
        elif has_failed:
            publication["status"] = PUB_PARTIAL
        else:
            publication["status"] = PUB_INTERRUPTED
        if was_interrupted and publication["status"] == PUB_COMPLETED:
            publication["history"].append({
                "seq": len(publication["history"]),
                "event": "interruption_recovered", "ts": time.time(),
            })
        publication["updated_at"] = time.time()

    @staticmethod
    def _terminal_state(publication: dict[str, Any]) -> str:
        return {
            PUB_RUNNING: "writing",
            PUB_PARTIAL: "partial_failed",
            PUB_COMPLETED: "completed",
            PUB_INTERRUPTED: "interrupted",
            PUB_FROZEN: "frozen",
        }.get(publication["status"], "unknown")

    def _upsert_landing(
        self,
        publication: dict[str, Any],
        metric: WindowMetric,
        partition: str,
        *,
        status: str,
    ) -> dict[str, Any]:
        """按 (发布, 窗口键) upsert 落地记录，保证一个窗口键仅一条记录。"""
        for row in self._landing_rows(publication["publication_id"]):
            if row["window_key"] == metric.window_key:
                # 已成功记录不可被覆盖（幂等保护）；失败记录允许原地更新
                if row["write_status"] == GRP_SUCCESS:
                    return row
                row["write_status"] = status
                row["partition"] = partition
                row["aggregates"] = dict(metric.aggregates)
                row["row_count"] = metric.row_count
                row["watermark"] = metric.watermark
                row["updated_at"] = time.time()
                return row
        record = {
            "record_id": self._next_id(),
            "publication_id": publication["publication_id"],
            "sink": publication["sink"],
            "version": publication["version"],
            "window_key": metric.window_key,
            "partition": partition,
            "aggregates": dict(metric.aggregates),
            "row_count": metric.row_count,
            "watermark": metric.watermark,
            "source_batch": publication["source_batch"],
            "write_status": status,
            "attempts": 1,
            "created_at": time.time(),
            "updated_at": time.time(),
        }
        if status == GRP_FAILED:
            record["last_error"] = f"分区 {partition} 写入失败"
        store.rows(LANDING_TABLE).append(record)
        return record

    # ------------------------------------------------------------------
    # 失败登记：序号3异常幂等，只记一次
    # ------------------------------------------------------------------

    def _register_group_failure(
        self, publication: dict[str, Any], record: dict[str, Any],
    ) -> None:
        idem = f"{publication['publication_id']}:{record['window_key']}"
        store.register_failure(
            module="sink_output",
            operation="window_landing",
            error=record.get("last_error", "窗口分组写入失败"),
            strategy="retry",
            context={
                "publication_id": publication["publication_id"],
                "window_key": record["window_key"],
                "partition": record.get("partition", ""),
                "version": publication["version"],
            },
            idempotency_key=idem,
        )

    def _resolve_group_failure(
        self, publication: dict[str, Any], record: dict[str, Any],
    ) -> None:
        idem = f"{publication['publication_id']}:{record['window_key']}"
        recovery_id = store._failure_idempotency.get(idem)
        if recovery_id:
            store.resolve_failure(recovery_id)

    def _register_publish_failure(
        self, publication: dict[str, Any], reason: str,
    ) -> None:
        idem = f"{publication['publication_id']}:publish"
        store.register_failure(
            module="sink_output",
            operation="publish_transaction",
            error=reason,
            strategy="checkpoint",
            context={
                "publication_id": publication["publication_id"],
                "version": publication["version"],
                "source_batch": publication["source_batch"],
            },
            idempotency_key=idem,
        )

    def _resolve_publish_failure(self, publication: dict[str, Any]) -> None:
        idem = f"{publication['publication_id']}:publish"
        recovery_id = store._failure_idempotency.get(idem)
        if recovery_id:
            store.resolve_failure(recovery_id)

    # ------------------------------------------------------------------
    # 输出装配
    # ------------------------------------------------------------------

    def _outcome(
        self,
        publication: dict[str, Any],
        report: dict[str, Any],
        *,
        mode: str,
        duplicate_skipped: int = 0,
        note: str = "",
    ) -> PublishOutcome:
        rows = self._landing_rows(publication["publication_id"])
        return PublishOutcome(
            publication_id=publication["publication_id"],
            sink=publication["sink"],
            version=publication["version"],
            mode=mode,
            total_groups=len(publication["batch_snapshot"]),
            success_groups=sum(1 for r in rows if r["write_status"] == GRP_SUCCESS),
            failed_groups=sum(1 for r in rows if r["write_status"] == GRP_FAILED),
            unique_groups=len({r["window_key"] for r in rows}),
            report_seq=report["report_seq"],
            terminal_state=report["terminal_state"],
            report=report,
            duplicate_skipped=duplicate_skipped,
            note=note,
        )

    def failure_groups(self, publication_id: str) -> list[dict[str, Any]]:
        """失败分组清单（锁定视图）：空发布返回空列表用于空态页。"""
        return [
            {
                "window_key": r["window_key"],
                "partition": r.get("partition", ""),
                "attempts": r.get("attempts", 1),
                "last_error": r.get("last_error", ""),
                "version": r["version"],
            }
            for r in self._landing_rows(publication_id)
            if r["write_status"] == GRP_FAILED
        ]

    def sink_summary(self, sink: str) -> dict[str, Any]:
        """sink 维度汇总：去重分组数量与成功文件保持一致。"""
        publications = self.list_publications(sink)
        consumable = self.list_consumable_records(sink)
        report = self.get_latest_report(sink)
        return {
            "sink": sink,
            "publications": len(publications),
            "latest_version": max((p["version"] for p in publications), default=0),
            "consumable_records": len(consumable),
            "unique_groups": len({r["window_key"] for r in consumable}),
            "success_files": len(consumable),
            "latest_report": report,
            "open_failures": len(store.get_open_failures("sink_output")),
        }


# 单例引擎
landing_engine = LandingRecoveryEngine()
