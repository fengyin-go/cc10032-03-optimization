"""结果落地恢复路由：窗口指标幂等落库、失败分组重试与完整性报告。

路径前缀 /api/sink-recovery，与通用资源路由 /api/{module} 并列，
固定路径在资源路由之外单独注册，避免动态路径抢匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.landing_recovery import (
    GRP_SUCCESS,
    WindowMetric,
    landing_engine,
)

router = APIRouter(prefix="/api/sink-recovery", tags=["结果落地恢复"])


class MetricPayload(BaseModel):
    window_key: str
    aggregates: dict[str, float] = Field(default_factory=dict)
    row_count: int = 0
    watermark: float = 0.0
    start_ts: float = 0.0
    end_ts: float = 0.0
    partition: str = ""
    source_batch: str = ""


class PublishPayload(BaseModel):
    sink: str
    source_batch: str | None = None
    metrics: list[MetricPayload]
    # 模拟故障：需要失败的分区/窗口键
    fail_partitions: list[str] = Field(default_factory=list)
    # 写入多少个分组后模拟中断（序号3中断写入）
    interrupt_after: int | None = None
    # 事务型失败：整批回滚不留半成品
    transaction_fail: bool = False


class RetryPayload(BaseModel):
    publication_id: str
    # 仍然不可用的分区，模拟分区持续故障；缺省表示故障已恢复
    still_failing: list[str] = Field(default_factory=list)
    # 重试期间到达的新窗口结果
    new_metrics: list[MetricPayload] = Field(default_factory=list)


class RepublishPayload(BaseModel):
    publication_id: str
    fail_partitions: list[str] = Field(default_factory=list)


class PublicationIdPayload(BaseModel):
    publication_id: str


def _to_metrics(items: list[MetricPayload]) -> list[WindowMetric]:
    return [
        WindowMetric(
            window_key=item.window_key,
            aggregates=item.aggregates,
            row_count=item.row_count,
            watermark=item.watermark,
            start_ts=item.start_ts,
            end_ts=item.end_ts,
            partition=item.partition,
            source_batch=item.source_batch,
        )
        for item in items
    ]


def _get_or_404(publication_id: str) -> dict[str, Any]:
    publication = landing_engine.get_publication(publication_id)
    if publication is None:
        raise HTTPException(status_code=404, detail=f"发布 {publication_id} 不存在")
    return publication


@router.post("/publish")
def publish(payload: PublishPayload) -> dict[str, Any]:
    """按源端批次发布窗口指标，返回发布结果与首版完整性报告。"""
    metrics = _to_metrics(payload.metrics)
    if not metrics:
        raise HTTPException(status_code=400, detail="窗口指标不能为空批次")
    outcome = landing_engine.publish_window_metrics(
        payload.sink,
        metrics,
        source_batch=payload.source_batch,
        fail_partitions=set(payload.fail_partitions),
        interrupt_after=payload.interrupt_after,
        transaction_fail=payload.transaction_fail,
    )
    return {
        "ok": True,
        "publication_id": outcome.publication_id,
        "sink": outcome.sink,
        "version": outcome.version,
        "mode": outcome.mode,
        "total_groups": outcome.total_groups,
        "success_groups": outcome.success_groups,
        "failed_groups": outcome.failed_groups,
        "unique_groups": outcome.unique_groups,
        "terminal_state": outcome.terminal_state,
        "report": outcome.report,
        "note": outcome.note,
    }


@router.get("/publications")
def list_publications(sink: str) -> dict[str, Any]:
    """列出某 sink 的全部发布（含失败/中断/冻结终态）。"""
    publications = landing_engine.list_publications(sink)
    return {
        "sink": sink,
        "total": len(publications),
        "items": [
            {
                "publication_id": p["publication_id"],
                "source_batch": p["source_batch"],
                "version": p["version"],
                "status": p["status"],
                "retry_count": p["retry_count"],
                "frozen": p["frozen"],
                "error_reason": p.get("error_reason", ""),
                "created_at": p["created_at"],
            }
            for p in publications
        ],
    }


@router.get("/records")
def consumable_records(sink: str) -> dict[str, Any]:
    """可消费落地记录：每个窗口键唯一一条，接口绝不返回半成品。"""
    records = landing_engine.list_consumable_records(sink)
    return {
        "sink": sink,
        "total": len(records),
        "unique_groups": len({r["window_key"] for r in records}),
        "items": records,
    }


@router.post("/retry")
def retry_failed(payload: RetryPayload) -> dict[str, Any]:
    """恢复路径A：锁定失败分组逐项补齐，复用已成功窗口键，同版本重新生成报告。"""
    _get_or_404(payload.publication_id)
    outcome = landing_engine.retry_failed_groups(
        payload.publication_id,
        still_failing=set(payload.still_failing),
        new_metrics=_to_metrics(payload.new_metrics),
    )
    return {
        "ok": True,
        "publication_id": outcome.publication_id,
        "sink": outcome.sink,
        "version": outcome.version,
        "mode": outcome.mode,
        "success_groups": outcome.success_groups,
        "failed_groups": outcome.failed_groups,
        "unique_groups": outcome.unique_groups,
        "duplicate_skipped": outcome.duplicate_skipped,
        "terminal_state": outcome.terminal_state,
        "report": outcome.report,
        "note": outcome.note,
    }


@router.post("/republish")
def freeze_republish(payload: RepublishPayload) -> dict[str, Any]:
    """恢复路径B：冻结整批后重新发布，分配连续新版本并重新生成报告。"""
    _get_or_404(payload.publication_id)
    outcome = landing_engine.freeze_and_republish(
        payload.publication_id,
        fail_partitions=set(payload.fail_partitions),
    )
    return {
        "ok": True,
        "publication_id": outcome.publication_id,
        "sink": outcome.sink,
        "version": outcome.version,
        "mode": outcome.mode,
        "success_groups": outcome.success_groups,
        "failed_groups": outcome.failed_groups,
        "unique_groups": outcome.unique_groups,
        "terminal_state": outcome.terminal_state,
        "report": outcome.report,
        "note": outcome.note,
    }


@router.post("/recover-transaction")
def recover_transaction(payload: PublicationIdPayload) -> dict[str, Any]:
    """事务失败恢复：用原始批次快照重新落地，恢复原结果且保持幂等。"""
    _get_or_404(payload.publication_id)
    outcome = landing_engine.recover_transaction(payload.publication_id)
    return {
        "ok": True,
        "publication_id": outcome.publication_id,
        "sink": outcome.sink,
        "version": outcome.version,
        "mode": outcome.mode,
        "success_groups": outcome.success_groups,
        "failed_groups": outcome.failed_groups,
        "unique_groups": outcome.unique_groups,
        "terminal_state": outcome.terminal_state,
        "report": outcome.report,
        "note": outcome.note,
    }


@router.get("/failures/{publication_id}")
def failure_groups(publication_id: str) -> dict[str, Any]:
    """失败分组清单（锁定视图）；无失败时返回空清单用于空态页。"""
    _get_or_404(publication_id)
    groups = landing_engine.failure_groups(publication_id)
    return {
        "publication_id": publication_id,
        "total": len(groups),
        "items": groups,
    }


@router.get("/reports/{publication_id}")
def publication_reports(publication_id: str) -> dict[str, Any]:
    """序号3详情页：同一发布下不同终态的完整性报告版本序列。"""
    _get_or_404(publication_id)
    reports = landing_engine.list_reports(publication_id)
    return {
        "publication_id": publication_id,
        "total": len(reports),
        "items": reports,
    }


@router.get("/summary")
def summary(sink: str) -> dict[str, Any]:
    """落库汇总：去重分组数量与成功文件一致，含最新同版本完整性报告。"""
    return landing_engine.sink_summary(sink)


@router.post("/demo/scenario3")
def demo_scenario3() -> dict[str, Any]:
    """一键演示序号3：分区失败 → 中断 → 并发重试只恢复一次 → 唯一分组与报告一致。

    沿流式源端调用链构造 6 个窗口分组，其中 2 个分区写入失败、第 4 组后中断，
    随后并发触发两次重试，验证异常只登记一次、报告只统计唯一分组。
    """
    import threading

    sink = "结果落地样例3"
    metrics = [
        MetricPayload(
            window_key=f"w3_{idx}",
            aggregates={"count": float(idx + 1), "sum": 10.0 * idx},
            row_count=idx + 1,
            watermark=1_700_000_000.0 + idx,
            partition=f"pt_{idx % 3:02d}",
            source_batch="stream_source_chain_3",
        )
        for idx in range(6)
    ]
    published = publish(PublishPayload(
        sink=sink,
        source_batch="stream_source_chain_3",
        metrics=metrics,
        fail_partitions=["pt_00"],
        interrupt_after=4,
    ))
    publication_id = published["publication_id"]

    results: list[dict[str, Any]] = []

    def _concurrent_retry() -> None:
        results.append(retry_failed(RetryPayload(publication_id=publication_id)))

    t1 = threading.Thread(target=_concurrent_retry)
    t2 = threading.Thread(target=_concurrent_retry)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    from app.store import store
    failure_records = [
        f for f in store._failures.values()
        if f.context.get("publication_id") == publication_id
    ]
    return {
        "ok": True,
        "sink": sink,
        "publication_id": publication_id,
        "initial_publish": published,
        "concurrent_retry_modes": [r["mode"] for r in results],
        "unique_failure_records": len({
            f.recovery_id for f in failure_records
        }),
        "failure_group_keys": sorted({
            f.context.get("window_key")
            for f in failure_records
            if f.context.get("window_key")
        }),
        "summary": landing_engine.sink_summary(sink),
        "latest_report": landing_engine.get_latest_report(sink),
    }
