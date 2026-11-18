"""结果落地服务封装层与管道集成测试。

验证：
  - SinkOutputService 委托恢复引擎后的对外行为（按窗口键唯一、同版本报告）
  - 停用 sink 拒绝写入、不存在 sink 报错、空批次空态
  - verify_output_integrity 只统计唯一分组并与可消费记录同版本
  - pipeline.sink_results_to_store 重复发布不产生重复窗口键
  - 内部落地表不进入模块清单与运营概览
"""
from __future__ import annotations

import pytest

from app.pipeline import (
    WindowResult,
    sink_results_to_store,
    verify_sink_integrity,
)
from app.services.landing_recovery import (
    LANDING_TABLE,
    PUBLICATION_TABLE,
    REPORT_TABLE,
    landing_engine,
)
from app.services.sink_output import SinkOutputService
from app.store import store


@pytest.fixture(autouse=True)
def clean_engine():
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


def _raw_results(count: int = 4) -> list[dict]:
    return [
        {
            "window_key": f"grp_{i}",
            "key": f"k{i}",
            "aggregates": {"sum": 10.0 * i},
            "row_count": i + 1,
            "watermark": 100.0 + i,
        }
        for i in range(count)
    ]


class TestSinkOutputServiceWrapper:
    def setup_method(self):
        self.service = SinkOutputService()

    def test_batch_write_publishes_unique_groups_with_report(self):
        result = self.service.batch_write_results(
            "结果落地样例1", _raw_results(), source_batch="svc-batch",
        )
        assert result["ok"] is True
        assert result["total"] == 4
        assert result["written"] == 4
        assert result["unique_groups"] == 4
        assert result["report"]["version"] == result["version"]
        assert result["report"]["consumable_matches_report"] is True

    def test_batch_write_with_failure_then_retry_is_idempotent(self):
        result = self.service.batch_write_results(
            "结果落地样例1",
            _raw_results(),
            source_batch="svc-fail",
            fail_partitions={"pt_00"},
        )
        assert result["failed"] >= 1
        publication_id = result["publication_id"]
        integrity = self.service.verify_output_integrity("结果落地样例1")
        assert integrity["needs_retry"] is True
        assert integrity["unique_groups"] == integrity["total"]

        retry = self.service.retry_failed_writes(publication_id)
        assert retry["version"] == result["version"]
        assert retry["report"]["complete"] is True
        # 重试后去重分组数量与成功文件一致
        integrity2 = self.service.verify_output_integrity("结果落地样例1")
        assert integrity2["success_files"] == integrity2["unique_groups"] == 4
        assert integrity2["needs_retry"] is False

    def test_disabled_sink_rejected(self):
        result = self.service.batch_write_results("结果落地样例3", _raw_results())
        assert result["ok"] is False
        assert "已停用" in result["error"]

    def test_unknown_sink_rejected(self):
        result = self.service.batch_write_results("不存在的落地", _raw_results())
        assert result["ok"] is False
        assert "不存在" in result["error"]

    def test_empty_batch_returns_empty_state(self):
        result = self.service.batch_write_results("结果落地样例1", [])
        assert result["ok"] is True
        assert result["total"] == 0
        integrity = self.service.verify_output_integrity("结果落地样例1")
        assert integrity["total"] == 0
        assert integrity["needs_retry"] is False

    def test_integrity_excludes_internal_tables(self):
        self.service.batch_write_results("结果落地样例1", _raw_results())
        overview = store.overview()
        internal = {
            name for name in store.module_names()
            if name.startswith("_sink_")
        }
        assert internal == set()
        # 概览里的 sink_output 行数仍是 3 条配置，落地内部记录不计入
        sink_module = next(m for m in overview["modules"] if m["name"] == "sink_output")
        assert sink_module["created"] == 3


class TestPipelineSinkIntegration:
    def _window_results(self) -> list[WindowResult]:
        return [
            WindowResult(
                window_key=f"w_{i}",
                start_ts=100.0 + i,
                end_ts=110.0 + i,
                aggregates={"u0_count": float(i)},
                row_count=i + 1,
                watermark=105.0 + i,
            )
            for i in range(5)
        ]

    def test_pipeline_sink_and_verify_unique(self):
        results = self._window_results()
        written, message = sink_results_to_store(results, "结果落地样例1")
        assert written == 5
        integrity = verify_sink_integrity(results)
        assert integrity["complete"] is True
        assert integrity["duplicate_keys"] == []

    def test_pipeline_republish_does_not_duplicate_window_keys(self):
        results = self._window_results()
        sink_results_to_store(results, "结果落地样例1")
        # 再次发布同窗口键（不同发布/版本），可消费集合仍按窗口键唯一
        sink_results_to_store(results, "结果落地样例1")
        consumable = landing_engine.list_consumable_records("结果落地样例1")
        keys = [r["window_key"] for r in consumable]
        assert len(keys) == len(set(keys)) == 5
        # 完整性核对仍只认唯一窗口键
        integrity = verify_sink_integrity(results)
        assert integrity["written"] == 5
        assert integrity["complete"] is True
