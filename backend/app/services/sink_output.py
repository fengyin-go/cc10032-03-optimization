"""结果落地领域服务：管理流式计算结果的输出落地配置。

数据流角色: state_store / calc_operator → sink_output（消费者+生产者）
增强能力: 批量写入调度、分区策略计算、写入模式切换、异常重试、输出完整性校验

恢复稳定性（本版重点）：
  批量写入按源端批次形成发布，落地记录按 (发布, 窗口键) 唯一；
  失败重试只补齐失败分组、复用已成功窗口键，绝不整批重写；
  完整性报告与可消费记录同版本且只统计唯一窗口键。
  详细状态机见 app/services/landing_recovery.py。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.landing_recovery import WindowMetric, landing_engine
from app.store import store


class SinkOutputService(BaseModuleService):
    MODULE = "sink_output"
    ENTITY = "输出配置"
    LIST_FIELDS = ["配置名称", "输出类型", "目标地址", "写入模式", "分区策略", "格式转换", "批量大小"]
    REQUIRED_FIELDS = ["配置名称", "输出类型", "目标地址"]
    STATUS_ORDER = ["草稿", "已生效", "已停用", "写入异常"]
    ACTION_RULES = {"启用输出": "草稿", "停用输出": "已生效"}
    NEGATIVE_ACTIONS = ["停用输出"]

    # ---- 跨模块数据消费与生产 ----

    def batch_write_results(
        self, sink_name: str, results: list[dict[str, Any]],
        *, source_batch: str | None = None,
        fail_partitions: set[str] | None = None,
    ) -> dict[str, Any]:
        """将计算结果按源端批次批量写入目标存储。

        每个结果项以 window_key（缺省取 key）作为唯一窗口键，委托恢复引擎
        执行幂等落库，返回发布版本与同版本完整性报告。
        """
        sink = None
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == sink_name:
                sink = r
                break
        if sink is None:
            return {"ok": False, "error": f"输出配置「{sink_name}」不存在"}
        if sink.get("status") == "已停用":
            return {"ok": False, "error": f"输出配置「{sink_name}」已停用"}

        partition_strategy = sink.get("分区策略", "round_robin")
        write_mode = sink.get("写入模式", "append")
        output_type = sink.get("输出类型", "database")
        format_type = sink.get("格式转换", "json")
        batch_id = source_batch or f"batch_{int(time.time() * 1000)}"

        metrics: list[WindowMetric] = []
        for index, item in enumerate(results):
            window_key = str(item.get("window_key", item.get("key", f"grp_{index}")))
            partition_key = self._calc_partition(item, partition_strategy, index)
            payload = item.get("aggregates", item)
            metrics.append(WindowMetric(
                window_key=window_key,
                aggregates=payload if isinstance(payload, dict) else {"value": payload},
                row_count=int(item.get("row_count", 1)),
                watermark=float(item.get("watermark", 0.0)),
                start_ts=float(item.get("start_ts", 0.0)),
                end_ts=float(item.get("end_ts", 0.0)),
                partition=partition_key,
                source_batch=batch_id,
            ))

        if not metrics:
            return {
                "ok": True, "sink": sink_name, "total": 0, "written": 0,
                "failed": 0, "partition_strategy": partition_strategy,
                "report": None, "note": "空批次，无落地记录",
            }

        outcome = landing_engine.publish_window_metrics(
            sink_name, metrics,
            source_batch=batch_id,
            fail_partitions=fail_partitions,
        )

        if outcome.failed_groups > 0:
            sink["status"] = "写入异常"
            sink["pending"] = True
        elif sink.get("status") == "草稿":
            sink["status"] = "已生效"

        return {
            "ok": True,
            "sink": sink_name,
            "publication_id": outcome.publication_id,
            "version": outcome.version,
            "total": outcome.total_groups,
            "written": outcome.success_groups,
            "failed": outcome.failed_groups,
            "unique_groups": outcome.unique_groups,
            "partition_strategy": partition_strategy,
            "write_mode": write_mode,
            "output_type": output_type,
            "format": format_type,
            "report": outcome.report,
        }

    @staticmethod
    def _calc_partition(
        item: dict[str, Any], strategy: str, index: int,
    ) -> str:
        """计算数据分区键。"""
        if strategy == "hash_key":
            raw = str(item.get("key", index))
            return f"pt_{hashlib.md5(raw.encode()).hexdigest()[:4]}"
        elif strategy == "time_range":
            ts = item.get("timestamp", time.time())
            return time.strftime("dt=%Y%m%d%H", time.localtime(ts))
        elif strategy == "source_based":
            return f"src_{item.get('source', 'default')}"
        else:  # round_robin
            return f"pt_{index % 8:02d}"

    @staticmethod
    def _convert_format(
        item: dict[str, Any], fmt: str,
    ) -> dict[str, Any]:
        """格式转换：JSON/CSV/Parquet 模拟。"""
        if fmt == "csv":
            return {"format": "csv", "raw": str(item)}
        elif fmt == "parquet":
            return {"format": "parquet", "columns": list(item.keys()), "rows": 1}
        return {"format": "json", "payload": item}

    def verify_output_integrity(self, sink_name: str) -> dict[str, Any]:
        """验证指定 sink 的输出完整性。

        只统计唯一窗口键，且与当前可消费记录使用同一版本；
        失败/冻结/中断的半成品不计入可消费集合。
        """
        summary = landing_engine.sink_summary(sink_name)
        report = summary.get("latest_report")
        if report is None:
            return {
                "ok": True,
                "sink": sink_name,
                "total": 0,
                "success": 0,
                "failed": 0,
                "success_rate": 0.0,
                "partitions": 0,
                "unique_groups": 0,
                "needs_retry": False,
                "version": None,
            }
        consumable = landing_engine.list_consumable_records(sink_name)
        return {
            "ok": True,
            "sink": sink_name,
            "version": report["version"],
            "report_seq": report["report_seq"],
            "terminal_state": report["terminal_state"],
            # 唯一分组口径：成功文件数 == 去重分组数
            "total": report["expected_groups"],
            "success": report["success_groups"],
            "failed": report["failed_groups"],
            "unique_groups": report["unique_groups"],
            "success_files": report["success_files"],
            "partitions": len({r.get("partition", "") for r in consumable}),
            "success_rate": report["integrity_rate"],
            "consumable_records": summary["consumable_records"],
            "consumable_matches_report": report["consumable_matches_report"],
            "needs_retry": report["failed_groups"] > 0,
            "complete": report["complete"],
        }

    def retry_failed_writes(
        self, publication_id: str, *, still_failing: set[str] | None = None,
    ) -> dict[str, Any]:
        """重试失败分组：只补齐失败窗口键，已成功分组复用，保持同版本。"""
        outcome = landing_engine.retry_failed_groups(
            publication_id, still_failing=still_failing,
        )
        return {
            "ok": True,
            "sink": outcome.sink,
            "publication_id": outcome.publication_id,
            "version": outcome.version,
            "total_failed": outcome.failed_groups,
            "retried_success": outcome.success_groups,
            "unique_groups": outcome.unique_groups,
            "mode": outcome.mode,
            "report": outcome.report,
            "note": outcome.note,
        }

    def list_sink_destinations(self) -> list[dict[str, Any]]:
        """列出所有活跃的输出目标和状态（仅配置行，id < 100）。"""
        configs = [
            r for r in store.rows(self.MODULE)
            if r.get("id", 0) < 100
        ]
        result = []
        for c in configs:
            name = c.get("配置名称", "")
            summary = landing_engine.sink_summary(name)
            result.append({
                "sink": name,
                "type": c.get("输出类型", ""),
                "target": c.get("目标地址", ""),
                "status": c.get("status", ""),
                "records": summary["consumable_records"],
                "unique_groups": summary["unique_groups"],
                "latest_version": summary["latest_version"],
            })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 只统计配置行，落地内部记录不计入模块数据集
        config_rows = [r for r in store.rows(self.MODULE) if r.get("id", 0) < 100]
        base["total_records"] = len(config_rows)
        base["failed_records"] = sum(
            1 for r in config_rows if r.get("status") == "写入异常"
        )
        return base
