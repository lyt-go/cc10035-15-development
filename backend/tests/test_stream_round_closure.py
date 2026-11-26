"""管道轮次封口、断点恢复、幂等签收和限定范围导出验收。"""
from __future__ import annotations

import pytest

from app.services.data_stream import DataStreamService
from app.services.round_ledger import round_ledger
from app.store import store


PIPELINE = "DATA-0003"
ROUND = "ROUND-DATA-0003-R1"


@pytest.fixture(autouse=True)
def clean_rounds():
    round_ledger.reset()
    # 轮次导出会在共享 batch_export 表留下关联任务，验收前清掉本测试产物。
    export_rows = store.rows("batch_export")
    export_rows[:] = [row for row in export_rows if row.get("round_id") != ROUND]
    yield
    round_ledger.reset()
    export_rows[:] = [row for row in export_rows if row.get("round_id") != ROUND]


def test_first_round_retains_ab_and_excludes_later_c():
    service = DataStreamService()

    assert service.receive_records(PIPELINE, ["A", "B"])["ok"]
    assert service.sign_item(PIPELINE, "A")["success_count"] == 1
    assert service.start_processing(PIPELINE, "B")["ok"]

    stopped = service.stop_pipeline(PIPELINE)
    assert stopped["ok"]
    assert stopped["round"]["round_id"] == ROUND
    assert stopped["round"]["closure_application"]["accepted_keys"] == ["A", "B"]

    arrived_after_close = service.receive_record(PIPELINE, "C")
    assert arrived_after_close["deferred"] is True
    assert arrived_after_close["deferred_keys"] == ["C"]

    assert service.interrupt_consumer(PIPELINE, item_key="B")["ok"]
    resumed = service.resume_consumer(application_id="CLOSE-" + ROUND)
    assert resumed["checkpoint_key"] == "A"
    assert resumed["pending_keys"] == ["B"]

    first_sign = service.sign_item(PIPELINE, "B")
    assert first_sign["success_count"] == 2
    duplicate_sign = service.sign_item(PIPELINE, "B")
    assert duplicate_sign["duplicate"] is True
    assert duplicate_sign["success_count"] == 2

    receipt = service.get_receipt(ROUND)["receipt"]
    assert receipt["accepted_scope"] == ["A", "B"]
    assert [item["item_key"] for item in receipt["items"]] == ["A", "B"]

    sealed = service.confirm_seal(ROUND)
    assert sealed["ok"]
    assert sealed["scope"]["keys"] == ["A", "B"]
    assert service.claim_scope(ROUND)["ok"]

    exported = service.export_receipt(ROUND)
    assert exported["ok"]
    assert exported["rows"] == ["A", "B"]
    assert "A" in exported["csv"] and "B" in exported["csv"]
    assert "C" not in exported["csv"]

    next_round = service.open_next_round(PIPELINE)
    assert next_round["round"]["round_no"] == 2
    assert next_round["assigned_deferred_keys"] == ["C"]
    assert service.sign_item(PIPELINE, "C")["round"]["round_no"] == 2

    old_export = service.export_receipt(ROUND)
    assert old_export["rows"] == ["A", "B"]
    assert "C" not in old_export["csv"]

    old_receipt = service.get_receipt(ROUND)["receipt"]
    assert old_receipt["final_scope"]["keys"] == ["A", "B"]
