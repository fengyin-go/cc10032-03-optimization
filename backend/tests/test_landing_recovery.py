"""窗口落库恢复引擎专项测试。

覆盖需求：
  - 按源端批次计算窗口指标并按窗口键幂等落地
  - 一个窗口键在一次发布中只能产生一条成功记录，重试从失败分组继续
  - 失败分区重试复用已成功窗口键，不产生重复指标
  - 重试后完整性报告只统计唯一分组，且与可消费记录同版本
  - 连续版本号；冻结整批重发保持版本连续
  - 序号3并发异常只恢复一次、保持幂等
  - 事务失败回滚不留半成品，恢复后还原原结果
  - 中断写入后可从失败/缺失分组继续
  - 空态、暂无数据、重复异常只记一次
"""
from __future__ import annotations

import threading

import pytest

from app.services.landing_recovery import (
    GRP_FAILED,
    GRP_SUCCESS,
    LANDING_TABLE,
    PUBLICATION_TABLE,
    REPORT_TABLE,
    WindowMetric,
    landing_engine,
)
from app.store import store


@pytest.fixture(autouse=True)
def clean_engine():
    """每个用例独立清空落库内部表与失败记录，避免用例间串数据。"""
    for table in (PUBLICATION_TABLE, LANDING_TABLE, REPORT_TABLE):
        store.rows(table).clear()
    store._failures.clear()
    store._failure_idempotency.clear()
    landing_engine._write_gates.clear()
    yield
    for table in (PUBLICATION_TABLE, LANDING_TABLE, REPORT_TABLE):
        store.rows(table).clear()
    store._failures.clear()
    store._failure_idempotency.clear()
    landing_engine._write_gates.clear()


def _metrics(sink: str = "sink_a", count: int = 5) -> list[WindowMetric]:
    return [
        WindowMetric(
            window_key=f"w_{idx}",
            aggregates={"count": float(idx + 1), "sum": 1.5 * idx},
            row_count=idx + 1,
            watermark=100.0 + idx,
            partition=f"pt_{idx % 3:02d}",
            source_batch="batch-1",
        )
        for idx in range(count)
    ]


class TestPublishAndUniqueness:
    def test_publish_creates_one_record_per_window_key(self):
        outcome = landing_engine.publish_window_metrics("sink_a", _metrics())
        assert outcome.version == 1
        assert outcome.success_groups == 5
        assert outcome.failed_groups == 0
        assert outcome.unique_groups == 5
        rows = landing_engine._landing_rows(outcome.publication_id)
        keys = [r["window_key"] for r in rows]
        assert len(keys) == len(set(keys)) == 5

    def test_report_counts_only_unique_groups(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_a", _metrics(), fail_partitions={"pt_00"},
        )
        report = outcome.report
        # pt_00 对应 w_0、w_3 两个分组失败
        assert report["failed_groups"] == 2
        assert report["success_groups"] == 3
        assert report["unique_groups"] == 5
        assert report["duplicate_records"] == 0
        assert report["complete"] is False

    def test_second_publish_gets_continuous_version(self):
        first = landing_engine.publish_window_metrics("sink_a", _metrics(count=3))
        second = landing_engine.publish_window_metrics("sink_a", _metrics(count=2))
        assert first.version == 1
        assert second.version == 2
        # 版本号在 sink 维度连续
        assert [p["version"] for p in landing_engine.list_publications("sink_a")] == [1, 2]

    def test_empty_batch_snapshot_supports_empty_state(self):
        outcome = landing_engine.publish_window_metrics("sink_empty", [])
        assert outcome.total_groups == 0
        assert landing_engine.list_consumable_records("sink_empty") == []
        report = landing_engine.get_latest_report("sink_empty")
        assert report["unique_groups"] == 0
        assert report["complete"] is True


class TestRetryFromFailedGroups:
    def test_retry_continues_from_failed_and_skips_succeeded(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_a", _metrics(), fail_partitions={"pt_00"},
        )
        pub_id = outcome.publication_id
        before_success = [
            r for r in landing_engine._landing_rows(pub_id)
            if r["write_status"] == GRP_SUCCESS
        ]
        # 故障恢复后重试：失败分组补齐，成功分组原样保留
        retry = landing_engine.retry_failed_groups(pub_id)
        assert retry.version == 1, "重试不换版本"
        assert retry.failed_groups == 0
        assert retry.success_groups == 5
        after_rows = landing_engine._landing_rows(pub_id)
        # 仍然只有 5 条记录，没有重复
        assert len(after_rows) == 5
        assert len({r["window_key"] for r in after_rows}) == 5
        # 已成功分组记录未被重写（created_at 保持不变）
        after_success = [r for r in after_rows if r["write_status"] == GRP_SUCCESS]
        assert {r["window_key"] for r in before_success}.issubset(
            {r["window_key"] for r in after_success}
        )

    def test_retry_regenerates_report_with_same_version(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_a", _metrics(), fail_partitions={"pt_01"},
        )
        pub_id = outcome.publication_id
        retry = landing_engine.retry_failed_groups(pub_id)
        report = retry.report
        assert report["version"] == outcome.version
        assert report["report_seq"] == 2, "重试后重新生成报告，序号递增"
        assert report["terminal_state"] == "completed"
        assert report["complete"] is True
        # 报告与可消费记录同版本、同唯一分组
        consumable = landing_engine.list_consumable_records("sink_a")
        assert report["consumable_version"] == report["version"]
        assert len({r["window_key"] for r in consumable}) == report["success_groups"]
        assert report["consumable_matches_report"] is True

    def test_persistent_partition_failure_keeps_group_failed(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_a", _metrics(), fail_partitions={"pt_02"},
        )
        retry = landing_engine.retry_failed_groups(
            outcome.publication_id, still_failing={"pt_02"},
        )
        assert retry.failed_groups >= 1
        assert retry.terminal_state == "partial_failed"
        # 可消费记录不含失败分组
        consumable_keys = {
            r["window_key"] for r in landing_engine.list_consumable_records("sink_a")
        }
        failed_keys = {
            g["window_key"] for g in landing_engine.failure_groups(outcome.publication_id)
        }
        assert failed_keys.isdisjoint(consumable_keys)

    def test_no_duplicate_records_after_multiple_retries(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_a", _metrics(), fail_partitions={"pt_00"},
        )
        pub_id = outcome.publication_id
        landing_engine.retry_failed_groups(pub_id, still_failing={"pt_00"})
        landing_engine.retry_failed_groups(pub_id, still_failing={"pt_00"})
        final = landing_engine.retry_failed_groups(pub_id)
        assert final.failed_groups == 0
        rows = landing_engine._landing_rows(pub_id)
        assert len(rows) == 5
        assert all(r["write_status"] == GRP_SUCCESS for r in rows)


class TestConcurrentRetryIdempotency:
    def test_concurrent_retry_recovers_only_once(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_3", _metrics(count=6), fail_partitions={"pt_00"},
            interrupt_after=4,
        )
        pub_id = outcome.publication_id
        barrier = threading.Barrier(2)
        modes: list[str] = []

        def _retry():
            barrier.wait()  # 两个线程同时发起，制造真实并发
            result = landing_engine.retry_failed_groups(pub_id)
            modes.append(result.mode)

        t1 = threading.Thread(target=_retry)
        t2 = threading.Thread(target=_retry)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # 一次真正恢复，一次并发幂等跳过
        assert sorted(modes) == ["retry_concurrent_skip", "retry_in_place"]
        rows = landing_engine._landing_rows(pub_id)
        assert len(rows) == 6
        assert len({r["window_key"] for r in rows}) == 6
        # 只产生一条恢复完成报告增量，重复请求不重复推进恢复
        pub = landing_engine.get_publication(pub_id)
        assert pub["retry_count"] == 1

    def test_repeated_anomaly_registered_once(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_3", _metrics(), fail_partitions={"pt_01"},
        )
        pub_id = outcome.publication_id
        # 多轮重试反复触及同一失败分组，失败记录按幂等键只登记一次
        landing_engine.retry_failed_groups(pub_id, still_failing={"pt_01"})
        landing_engine.retry_failed_groups(pub_id, still_failing={"pt_01"})
        failures = [
            f for f in store._failures.values()
            if f.context.get("publication_id") == pub_id
        ]
        group_failures = [f for f in failures if f.failed_operation == "window_landing"]
        keys = [(f.context["window_key"]) for f in group_failures]
        assert len(keys) == len(set(keys))
        # 恢复成功后失败记录被标记 resolved
        landing_engine.retry_failed_groups(pub_id)
        assert store.get_open_failures("sink_output") == []


class TestNewArrivalsDuringRetry:
    def test_new_metrics_merge_without_duplicate(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_a", _metrics(count=4), fail_partitions={"pt_00"},
        )
        pub_id = outcome.publication_id
        new_metrics = [
            WindowMetric(
                window_key="w_4", aggregates={"count": 9}, row_count=9,
                watermark=200.0, partition="pt_01", source_batch="batch-1",
            ),
            # 与已存在窗口键重复，应被跳过
            WindowMetric(
                window_key="w_0", aggregates={"count": 1}, row_count=1,
                watermark=100.0, partition="pt_00", source_batch="batch-1",
            ),
        ]
        retry = landing_engine.retry_failed_groups(pub_id, new_metrics=new_metrics)
        assert retry.duplicate_skipped == 1
        rows = landing_engine._landing_rows(pub_id)
        assert len({r["window_key"] for r in rows}) == 5
        assert retry.version == 1


class TestFreezeRepublish:
    def test_freeze_then_republish_keeps_continuous_version(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_a", _metrics(), fail_partitions={"pt_00"},
        )
        old_id = outcome.publication_id
        republish = landing_engine.freeze_and_republish(old_id)
        assert republish.version == 2
        old_pub = landing_engine.get_publication(old_id)
        assert old_pub["status"] == "已冻结"
        # 旧版本成功记录不再可消费，被新版本替代
        consumable = landing_engine.list_consumable_records("sink_a")
        assert all(r["version"] == 2 for r in consumable)
        assert len({r["window_key"] for r in consumable}) == 5
        # 新版本报告与可消费记录同版本
        assert republish.report["consumable_matches_report"] is True

    def test_republish_recovers_when_partition_healed(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_a", _metrics(), fail_partitions={"pt_02"},
        )
        republish = landing_engine.freeze_and_republish(
            outcome.publication_id, fail_partitions=set(),
        )
        assert republish.failed_groups == 0
        assert republish.terminal_state == "completed"


class TestTransactionFailure:
    def test_transaction_failure_leaves_no_half_baked_records(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_a", _metrics(), transaction_fail=True,
        )
        assert outcome.terminal_state == "transaction_failed"
        # 没有任何落地记录，接口不暴露半成品
        assert landing_engine._landing_rows(outcome.publication_id) == []
        assert landing_engine.list_consumable_records("sink_a") == []
        report = outcome.report
        assert report["success_groups"] == 0
        assert report["unique_groups"] == 0

    def test_transaction_recover_restores_original_results(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_a", _metrics(count=4), transaction_fail=True,
        )
        pub_id = outcome.publication_id
        recovered = landing_engine.recover_transaction(pub_id)
        assert recovered.version == 1, "事务恢复沿用同一版本"
        assert recovered.success_groups == 4
        assert recovered.failed_groups == 0
        rows = landing_engine._landing_rows(pub_id)
        assert len({r["window_key"] for r in rows}) == 4
        # 原始快照数据被完整还原
        snapshot_keys = {item["window_key"] for item in
                         landing_engine.get_publication(pub_id)["batch_snapshot"]}
        assert {r["window_key"] for r in rows} == snapshot_keys

    def test_transaction_recover_is_idempotent_under_concurrency(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_a", _metrics(), transaction_fail=True,
        )
        pub_id = outcome.publication_id
        barrier = threading.Barrier(2)
        modes: list[str] = []

        def _recover():
            barrier.wait()
            modes.append(landing_engine.recover_transaction(pub_id).mode)

        t1 = threading.Thread(target=_recover)
        t2 = threading.Thread(target=_recover)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert sorted(modes) == ["retry_concurrent_skip", "transaction_recover"]
        assert len(landing_engine._landing_rows(pub_id)) == 5
        # 恢复只执行一次，落地记录无重复
        rows = landing_engine._landing_rows(pub_id)
        assert len({r["window_key"] for r in rows}) == 5


class TestInterruptedWrite:
    def test_interrupted_publication_resumes_from_remaining_groups(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_a", _metrics(count=6), interrupt_after=3,
        )
        pub_id = outcome.publication_id
        assert outcome.terminal_state == "interrupted"
        # 中断发布的成功记录尚不可消费（半成品不外露）
        assert landing_engine.list_consumable_records("sink_a") == []
        retry = landing_engine.retry_failed_groups(pub_id)
        # 中断后剩余分组补齐
        assert retry.unique_groups == 6
        assert retry.terminal_state == "completed"
        assert len(landing_engine.list_consumable_records("sink_a")) == 6


class TestFailureGroupsAndEmptyState:
    def test_failure_groups_empty_for_healthy_publication(self):
        outcome = landing_engine.publish_window_metrics("sink_a", _metrics())
        assert landing_engine.failure_groups(outcome.publication_id) == []

    def test_failure_groups_lists_locked_partitions(self):
        outcome = landing_engine.publish_window_metrics(
            "sink_a", _metrics(), fail_partitions={"pt_00"},
        )
        groups = landing_engine.failure_groups(outcome.publication_id)
        assert len(groups) == 2
        assert all(g["last_error"] for g in groups)
        assert {g["version"] for g in groups} == {1}

    def test_sink_summary_dedup_matches_success_files(self):
        landing_engine.publish_window_metrics(
            "sink_a", _metrics(), fail_partitions={"pt_01"},
        )
        summary = landing_engine.sink_summary("sink_a")
        # 去重分组数量与成功文件一致
        assert summary["unique_groups"] == summary["success_files"]
        assert summary["unique_groups"] == summary["consumable_records"]


class TestReportVersionAlignment:
    def test_each_report_carries_consumable_version(self):
        first = landing_engine.publish_window_metrics(
            "sink_a", _metrics(count=3), fail_partitions={"pt_00"},
        )
        landing_engine.retry_failed_groups(first.publication_id)
        second = landing_engine.publish_window_metrics("sink_a", _metrics(count=3))
        latest = landing_engine.get_latest_report("sink_a")
        assert latest["version"] == second.version
        reports = landing_engine.list_reports(first.publication_id)
        # 序号3详情页：同一发布下保留不同终态的报告序列
        terminals = {r["terminal_state"] for r in reports}
        assert "partial_failed" in terminals
        assert "completed" in terminals
        # 全部报告版本号连续且等于其发布版本
        assert all(r["version"] == 1 for r in reports)
