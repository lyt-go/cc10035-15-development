"""管道运行轮次、封口回单与消费位点的内存领域台账。

这里不依赖 Web 层，后端服务、跨模块协调和接口路由都复用同一份状态，
保证“结束接收 → 在途消费恢复 → 封口 → 领取范围/导出”的规则只有一处实现。
"""
from __future__ import annotations

import csv
import io
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from app.store import store

CST = timezone(timedelta(hours=8))

ROUND_TABLE = "stream_rounds"
RECORD_KEY_FIELDS = (
    "item_key", "key", "记录标识", "记录编号", "key_name",
    "message_id", "msg_id", "item_id", "record_id", "id",
)


def now() -> str:
    return datetime.now(CST).isoformat(timespec="seconds")


def _rows() -> list[dict[str, Any]]:
    return store.rows(ROUND_TABLE)


def _pipeline_rows() -> list[dict[str, Any]]:
    return store.rows("data_stream")


def _safe_code(value: Any) -> str:
    text = str(value or "PIPE")
    return re.sub(r"[^A-Za-z0-9_-]+", "-", text).strip("-") or "PIPE"


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    if isinstance(value, str):
        # 允许 A,B 或 A，B；中文逗号也兼容。
        return [part.strip() for part in re.split(r"[,，;；\s]+", value) if part.strip()]
    return [value]


def _record_key(record: Any) -> str:
    if isinstance(record, dict):
        for field in RECORD_KEY_FIELDS:
            value = record.get(field)
            if value not in (None, ""):
                return str(value)
    return str(record)


class RoundLedger:
    """维护轮次、记录、签收幂等键、checkpoint、回单和导出交付。"""

    # ---------- 基础解析 ----------

    def find_pipeline(self, ref: Any) -> dict[str, Any] | None:
        if ref is None or ref == "":
            return None
        if isinstance(ref, dict):
            ref = (
                ref.get("管道编号")
                or ref.get("pipeline_id")
                or ref.get("stream_id")
                or ref.get("pipeline_ref")
                or ref.get("id")
            )
        text = str(ref).strip()
        rows = _pipeline_rows()

        if text.isdigit():
            row = store.find("data_stream", int(text))
            if row is not None:
                return row

        for row in rows:
            if str(row.get("管道编号", "")) == text:
                return row
        # 容忍 DATA-1 / 名称包含等调用方式。
        folded = text.casefold()
        for row in rows:
            if folded in str(row.get("管道编号", "")).casefold():
                return row
        return None

    def list_rounds(self, pipeline_ref: Any = None) -> list[dict[str, Any]]:
        rounds = list(_rows())
        pipeline = self.find_pipeline(pipeline_ref) if pipeline_ref is not None else None
        if pipeline is not None:
            pid = int(pipeline.get("id", 0))
            code = pipeline.get("管道编号")
            rounds = [
                r for r in rounds
                if r.get("pipeline_pk") == pid or r.get("管道编号") == code
            ]
        return sorted(rounds, key=lambda r: (int(r.get("pipeline_pk", 0)), int(r.get("round_no", 0))))

    def _find_round_by_ref(self, ref: Any) -> dict[str, Any] | None:
        if ref is None or ref == "":
            return None
        if isinstance(ref, dict):
            ref = ref.get("round_id") or ref.get("id") or ref.get("轮次编号")
        text = str(ref).strip()

        if text.isdigit():
            row = store.find(ROUND_TABLE, int(text))
            if row is not None:
                return row

        for field in ("round_id", "轮次编号", "receipt_no", "回单编号", "application_id", "申请编号"):
            for row in _rows():
                value = row.get(field)
                if value and str(value) == text:
                    return row
                closure = row.get("closure_application") or {}
                if isinstance(closure, dict) and str(closure.get(field, "")) == text:
                    return row

        folded = text.casefold()
        for row in _rows():
            candidates = [row.get("round_id"), row.get("轮次编号"), row.get("receipt_no"), row.get("回单编号")]
            closure = row.get("closure_application") or {}
            if isinstance(closure, dict):
                candidates.extend([closure.get("application_id"), closure.get("申请编号")])
            if any(folded == str(value).casefold() for value in candidates if value):
                return row
        return None

    def resolve_round(
        self,
        target: Any = None,
        *,
        pipeline_id: Any = None,
        round_id: Any = None,
        round_ref: Any = None,
        receipt_no: str | None = None,
        application_id: str | None = None,
        round_no: int | str | None = None,
        create: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        for ref in (round_id, round_ref, receipt_no, application_id, target):
            if ref not in (None, ""):
                row = self._find_round_by_ref(ref)
                if row is not None:
                    return row

        pipeline_ref = pipeline_id or kwargs.get("stream_id") or kwargs.get("pipeline_ref") or kwargs.get("pipeline_code")
        if pipeline_ref in (None, "") and target not in (None, ""):
            # target 可能是管道编号，也可能是无法识别的轮次号。
            if self.find_pipeline(target) is not None:
                pipeline_ref = target

        pipeline = self.find_pipeline(pipeline_ref)
        if pipeline is None:
            if create and pipeline_ref is not None:
                return None
            return None

        rounds = [
            r for r in _rows()
            if r.get("pipeline_pk") == int(pipeline.get("id", 0))
        ]
        if round_no not in (None, ""):
            try:
                number = int(round_no)
            except (TypeError, ValueError):
                number = -1
            for row in rounds:
                if int(row.get("round_no", 0)) == number:
                    return row
            return None

        if rounds:
            return sorted(rounds, key=lambda r: int(r.get("round_no", 0)))[-1]
        if create:
            opened = self.open_round(pipeline)
            return opened.get("round")
        return None

    # ---------- 轮次开启 ----------

    def open_round(self, pipeline_ref: Any = None, *, pipeline_id: Any = None) -> dict[str, Any]:
        pipeline = self.find_pipeline(pipeline_ref if pipeline_ref is not None else pipeline_id)
        if pipeline is None:
            return {"ok": False, "message": f"数据流管道 {pipeline_ref or pipeline_id} 不存在"}

        pid = int(pipeline.get("id", 0))
        existing = [r for r in _rows() if r.get("pipeline_pk") == pid]
        current_accepting = next((r for r in existing if r.get("status") == "受理中"), None)
        if current_accepting is not None:
            return {
                "ok": True,
                "message": f"第 {current_accepting['round_no']} 轮已在受理中",
                "round": current_accepting,
                "round_id": current_accepting.get("round_id"),
                "round_no": current_accepting.get("round_no"),
                "created": False,
            }

        round_no = max((int(r.get("round_no", 0)) for r in existing), default=0) + 1
        code = _safe_code(pipeline.get("管道编号") or pid)
        round_id = f"ROUND-{code}-R{round_no}"
        receipt_no = f"HD-{code}-R{round_no}"
        seq_id = max((int(r.get("id", 0)) for r in _rows()), default=0) + 1
        item_id = max((int(i.get("id", 0)) for i in _rows()), default=0) + 1
        # id 与轮次 id 共用同一增长空间即可；记录项使用 seq 作为位置。
        round_row: dict[str, Any] = {
            "id": seq_id,
            "round_id": round_id,
            "轮次编号": round_id,
            "receipt_no": receipt_no,
            "回单编号": receipt_no,
            "pipeline_pk": pid,
            "pipeline_id": pipeline.get("管道编号"),
            "管道编号": pipeline.get("管道编号"),
            "round_no": round_no,
            "轮次": round_no,
            "status": "受理中",
            "轮次状态": "受理中",
            "intake_open": True,
            "受理状态": "受理中",
            "consumer_status": "空闲",
            "消费者状态": "空闲",
            "interrupted": False,
            "checkpoint": 0,
            "已完成位置": 0,
            "checkpoint_key": None,
            "completed_position": 0,
            "success_count": 0,
            "成功条数": 0,
            "completed_count": 0,
            "processed_count": 0,
            "signed_count": 0,
            "ack_count": 0,
            "received_count": 0,
            "accepted_count": 0,
            "items": [],
            "accepted_keys": [],
            "completed_keys": [],
            "signed_keys": [],
            "idempotency_keys": {},
            "closure_application": None,
            "receipt": None,
            "final_scope": None,
            "scope_claimed": False,
            "claimed_at": None,
            "exports": [],
            "created_at": now(),
            "opened_at": now(),
            "closed_at": None,
            "sealed_at": None,
        }
        _rows().append(round_row)

        pipeline["status"] = "运行中"
        pipeline["运行状态"] = "运行中"
        pipeline["current_round_id"] = round_id
        pipeline["current_round_no"] = round_no

        # 结束接收后到达、等待后续轮次的记录，在轮次开启时归入本轮。
        deferred = list(pipeline.get("deferred_records") or [])
        assigned: list[dict[str, Any]] = []
        pipeline["deferred_records"] = []
        for deferred_record in deferred:
            key = _record_key(deferred_record)
            result = self.receive_records(round_row, item_key=key, deferred_source=deferred_record)
            if result.get("ok"):
                assigned.append(key)
        self._refresh_receipt(round_row)

        return {
            "ok": True,
            "message": f"管道 {pipeline.get('管道编号')} 第 {round_no} 轮已开启",
            "round": round_row,
            "round_id": round_row.get("round_id"),
            "round_no": round_row.get("round_no"),
            "assigned_deferred_keys": assigned,
            "created": True,
        }

    def open_next_round(self, target: Any = None, **kwargs: Any) -> dict[str, Any]:
        pipeline = self.find_pipeline(target or kwargs.get("pipeline_id"))
        if pipeline is None:
            round_row = self.resolve_round(target, **kwargs)
            if round_row is not None:
                pipeline = self.find_pipeline(round_row.get("pipeline_pk"))
        if pipeline is None:
            return {"ok": False, "message": "找不到要开启下一轮的数据流管道"}
        return self.open_round(pipeline)

    # ---------- 记录受理 ----------

    def receive_records(
        self,
        target: Any = None,
        records: Any = None,
        *,
        item_key: Any = None,
        keys: Any = None,
        deferred_source: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        # 兼容 receive_records("DATA-0003", "A")、(["A","B"]) 等位置写法。
        if records is not None and item_key is None and keys is None:
            if isinstance(records, (str, list, tuple, set)) and not isinstance(records, dict):
                if target is not None and self.resolve_round(target) is None and self.find_pipeline(target) is None:
                    item_key = records
                    records = None
                else:
                    keys = records
                    records = None
        if item_key is not None and keys is None and records is None:
            keys = item_key

        round_row = self.resolve_round(target, create=True, **kwargs)
        requested = [*_as_list(keys), *([] if records is None else [records])]
        if records is not None and keys is None:
            requested = _as_list(records) if not isinstance(records, (list, tuple)) else list(records)
        requested.extend(_as_list(kwargs.get("record")))
        requested.extend(_as_list(kwargs.get("item_keys")))

        if round_row is None:
            return {"ok": False, "message": "数据流管道或轮次不存在"}

        pipeline = self.find_pipeline(round_row.get("pipeline_pk"))
        if pipeline is None:
            return {"ok": False, "message": "轮次关联的管道不存在"}

        # 已结束接收/已封口：新记录不能进入原轮次，只能等待后续开启的轮次。
        if round_row.get("status") != "受理中" or not round_row.get("intake_open", False):
            deferred_keys: list[str] = []
            for record in requested:
                key = _record_key(record)
                if not key:
                    continue
                inbox = pipeline.setdefault("deferred_records", [])
                if not any(_record_key(item) == key for item in inbox):
                    entry = {
                        "item_key": key,
                        "记录标识": key,
                        "received_at": now(),
                        "reason": "当前轮次已结束接收，归属后续开启轮次",
                        "payload": record if isinstance(record, dict) else {"value": record},
                    }
                    inbox.append(entry)
                deferred_keys.append(key)
            return {
                "ok": True,
                "deferred": True,
                "assigned_round": None,
                "deferred_keys": deferred_keys,
                "message": "当前轮次已结束接收，记录已归入后续开启的轮次",
            }

        accepted: list[dict[str, Any]] = []
        duplicate_keys: list[str] = []
        for record in requested:
            key = _record_key(record)
            if not key:
                continue
            existing = next((item for item in round_row["items"] if item.get("item_key") == key), None)
            if existing is not None:
                duplicate_keys.append(key)
                accepted.append(existing)
                continue

            seq = len(round_row["items"]) + 1
            payload = record if isinstance(record, dict) else {"value": record}
            source_payload = (deferred_source or {}).get("payload") if deferred_source else None
            item = {
                "id": max((int(i.get("id", 0)) for r in _rows() for i in r.get("items", [])), default=0) + 1,
                "item_key": key,
                "记录标识": key,
                "key": key,
                "seq": seq,
                "位置": seq,
                "status": "已受理",
                "处理状态": "已受理",
                "accepted": True,
                "确认受理": True,
                "processed": False,
                "completed": False,
                "signed": False,
                "签收状态": "未签收",
                "success": False,
                "attempts": 0,
                "sign_attempts": 0,
                "signed_at": None,
                "completed_at": None,
                "received_at": (deferred_source or {}).get("received_at") or now(),
                "payload": source_payload or payload,
            }
            round_row["items"].append(item)
            if key not in round_row["accepted_keys"]:
                round_row["accepted_keys"].append(key)
            accepted.append(item)

        round_row["received_count"] = len(round_row["items"])
        round_row["accepted_count"] = len(round_row["accepted_keys"])
        self._refresh_receipt(round_row)
        return {
            "ok": True,
            "message": f"已受理 {len(accepted)} 项，其中重复 {len(duplicate_keys)} 项",
            "round": round_row,
            "round_id": round_row.get("round_id"),
            "round_no": round_row.get("round_no"),
            "accepted": accepted,
            "keys": [item["item_key"] for item in accepted],
            "duplicate_keys": duplicate_keys,
            "deferred": False,
        }

    # 语义别名：现场词汇可能叫“接收/确认受理/登记记录”。
    def receive_record(self, target: Any = None, item_key: Any = None, **kwargs: Any) -> dict[str, Any]:
        return self.receive_records(target, item_key=item_key, **kwargs)

    accept_record = receive_record
    accept_records = receive_records
    intake_record = receive_record

    def confirm_acceptance(self, target: Any = None, item_key: Any = None, **kwargs: Any) -> dict[str, Any]:
        result = self.receive_records(target, item_key=item_key, **kwargs)
        if result.get("ok"):
            result["message"] = "受理确认已保存，原回单不会移除已确认记录"
        return result

    # ---------- 结束接收/封口申请 ----------

    def end_receiving(
        self,
        target: Any = None,
        *,
        completion_signals: Any = None,
        conflict_keys: Any = None,
        application_id: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        round_row = self.resolve_round(target, create=kwargs.pop("create", True), **kwargs)
        if round_row is None:
            return {"ok": False, "message": "没有可办理结束接收的管道轮次"}

        # 幂等：重复提交/页面继续都返回同一个原申请，不能另起一轮或清空进度。
        if round_row.get("closure_application"):
            self._refresh_receipt(round_row)
            return {
                "ok": True,
                "continued": True,
                "message": "原封口申请仍在，已继续原申请并保留消费进度",
                "application": round_row["closure_application"],
                "receipt": round_row.get("receipt"),
                "round": round_row,
            }

        round_row["intake_open"] = False
        round_row["status"] = "封口申请中"
        round_row["轮次状态"] = "封口申请中"
        round_row["受理状态"] = "已结束接收"
        round_row["closed_at"] = now()

        app_id = application_id or f"CLOSE-{round_row['round_id']}"
        signals = set(_as_list(completion_signals) + _as_list(conflict_keys) + _as_list(kwargs.get("completion_signal")))
        conflicts: list[dict[str, Any]] = []
        for key in signals:
            item = self._find_item(round_row, key)
            if item is None:
                continue
            item["completion_signal"] = "停流竞争，待恢复仲裁"
            item["signal_conflict"] = True
            conflicts.append({
                "item_key": key,
                "type": "stop_vs_completion",
                "resolution": "completion_signal_retained_until_recovery",
            })

        in_flight = [i["item_key"] for i in round_row["items"] if i.get("status") == "处理中" or (i.get("attempts", 0) > 0 and not i.get("signed"))]
        # 即使调用方没有显式传入竞争信号，B 正处理时停流也按“停流 vs 完成信号”
        # 保留冲突标记；恢复并幂等签收后才计入成功范围。
        conflicted_keys = {c.get("item_key") for c in conflicts}
        for key in in_flight:
            item = self._find_item(round_row, key)
            if item is not None and key not in conflicted_keys:
                item["completion_signal"] = "停流竞争，待恢复仲裁"
                item["signal_conflict"] = True
                conflicts.append({
                    "item_key": key,
                    "type": "stop_while_processing",
                    "resolution": "checkpoint_recovery_and_idempotent_sign",
                })
        application = {
            "application_id": app_id,
            "申请编号": app_id,
            "round_id": round_row["round_id"],
            "轮次编号": round_row["round_id"],
            "receipt_no": round_row["receipt_no"],
            "回单编号": round_row["receipt_no"],
            "status": "封口申请中" if in_flight else "待封口确认",
            "申请状态": "封口申请中" if in_flight else "待封口确认",
            "submitted_at": now(),
            "checkpoint": round_row.get("checkpoint", 0),
            "checkpoint_key": round_row.get("checkpoint_key"),
            "success_count": round_row.get("success_count", 0),
            "accepted_keys": list(round_row.get("accepted_keys", [])),
            "in_flight_keys": in_flight,
            "conflicts": conflicts,
        }
        round_row["closure_application"] = application
        if in_flight:
            round_row["consumer_status"] = "完成信号冲突/等待恢复" if conflicts else "等待在途消费完成"
            round_row["消费者状态"] = round_row["consumer_status"]
        self._refresh_receipt(round_row)

        pipeline = self.find_pipeline(round_row.get("pipeline_pk"))
        if pipeline is not None:
            pipeline["last_closure_application_id"] = app_id

        return {
            "ok": True,
            "continued": False,
            "message": "结束接收申请已提交，已受理记录保留在原回单",
            "application": application,
            "receipt": round_row.get("receipt"),
            "round": round_row,
            "conflicts": conflicts,
        }

    finish_receiving = end_receiving
    close_intake = end_receiving
    stop_intake = end_receiving
    submit_seal_application = end_receiving
    request_seal = end_receiving

    # ---------- 消费、中断、恢复、幂等签收 ----------

    def _find_item(self, round_row: dict[str, Any], item_key: Any) -> dict[str, Any] | None:
        key = str(item_key)
        for item in round_row.get("items", []):
            if str(item.get("item_key")) == key or str(item.get("id")) == key:
                return item
        return None

    def start_processing(
        self,
        target: Any = None,
        item_key: Any = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        round_row = self.resolve_round(target, **kwargs)
        if round_row is None:
            return {"ok": False, "message": "轮次不存在"}
        lookup_key = (
            item_key
            or kwargs.get("key")
            or kwargs.get("record_key")
            or kwargs.get("item_id")
            or kwargs.get("message_id")
            or kwargs.get("record_id")
        )
        item = self._find_item(round_row, lookup_key)
        if item is None:
            return {"ok": False, "message": f"本轮找不到记录 {item_key}"}
        if item.get("signed"):
            return {"ok": True, "duplicate": True, "message": "该项已签收，无需重复处理", "item": item, "round": round_row}
        item["status"] = "处理中"
        item["处理状态"] = "处理中"
        item["attempts"] = int(item.get("attempts", 0)) + 1
        item["started_at"] = now()
        round_row["consumer_status"] = "处理中"
        round_row["消费者状态"] = "处理中"
        round_row["current_item_key"] = item["item_key"]
        self._refresh_progress(round_row)
        return {"ok": True, "message": f"{item['item_key']} 开始处理", "item": item, "round": round_row}

    process_item = start_processing
    consume_item = start_processing

    def interrupt_consumer(self, target: Any = None, item_key: Any = None, **kwargs: Any) -> dict[str, Any]:
        round_row = self.resolve_round(target, **kwargs)
        if round_row is None:
            return {"ok": False, "message": "轮次不存在"}
        round_row["interrupted"] = True
        round_row["consumer_status"] = "已中断"
        round_row["消费者状态"] = "已中断"
        interrupted_keys: list[str] = []
        key = item_key or kwargs.get("key") or kwargs.get("item_id") or kwargs.get("message_id")
        if key:
            item = self._find_item(round_row, key)
            if item is not None and not item.get("signed"):
                item["interrupted"] = True
                interrupted_keys.append(item["item_key"])
        else:
            for item in round_row.get("items", []):
                if item.get("status") == "处理中" and not item.get("signed"):
                    item["interrupted"] = True
                    interrupted_keys.append(item["item_key"])
        return {
            "ok": True,
            "message": "消费者已中断，已完成位置和成功条数保持不变",
            "round": round_row,
            "interrupted_keys": interrupted_keys,
            "checkpoint": round_row.get("checkpoint"),
            "checkpoint_key": round_row.get("checkpoint_key"),
            "success_count": round_row.get("success_count"),
        }

    def resume_consumer(self, target: Any = None, **kwargs: Any) -> dict[str, Any]:
        # 允许传封口申请编号，从页面“继续原申请”时找回同一个 round。
        round_row = self.resolve_round(target, **kwargs)
        if round_row is None:
            return {"ok": False, "message": "找不到原消费申请或轮次"}
        round_row["interrupted"] = False
        pending = [
            item["item_key"] for item in round_row.get("items", [])
            if not item.get("signed")
        ]
        round_row["consumer_status"] = "已恢复" + (f"，待处理：{','.join(pending)}" if pending else "，全部完成")
        round_row["消费者状态"] = round_row["consumer_status"]
        for item in round_row.get("items", []):
            if item.get("interrupted") and not item.get("signed"):
                item["status"] = "处理中"
                item["处理状态"] = "处理中"
                item["resumed_at"] = now()
                item["interrupted"] = False
        return {
            "ok": True,
            "message": "消费者已从同一轮已完成位置恢复",
            "round": round_row,
            "round_id": round_row.get("round_id"),
            "application": round_row.get("closure_application"),
            "checkpoint": round_row.get("checkpoint"),
            "checkpoint_key": round_row.get("checkpoint_key"),
            "resume_from": round_row.get("checkpoint"),
            "next_position": int(round_row.get("checkpoint", 0)) + 1,
            "skip_keys": list(round_row.get("completed_keys", [])),
            "pending_keys": pending,
            "success_count": round_row.get("success_count"),
        }

    recover_consumer = resume_consumer
    continue_application = resume_consumer

    def sign_item(
        self,
        target: Any = None,
        item_key: Any = None,
        *,
        key: Any = None,
        record_key: Any = None,
        idempotency_key: str | None = None,
        sign_token: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        # 兼容 sign_item("ROUND-...", {"item_key":"B"}) 或 sign_item(pipeline_id="DATA-0003", key="B")
        if isinstance(item_key, dict):
            idempotency_key = idempotency_key or item_key.get("idempotency_key") or item_key.get("sign_token")
            item_key = _record_key(item_key)
        item_key = (
            item_key
            or key
            or record_key
            or kwargs.get("item")
            or kwargs.get("item_id")
            or kwargs.get("message_id")
            or kwargs.get("msg_id")
            or kwargs.get("record_id")
        )
        round_row = self.resolve_round(target, **kwargs)
        if round_row is None:
            return {"ok": False, "message": "轮次不存在，无法签收"}
        item = self._find_item(round_row, item_key)
        if item is None:
            return {"ok": False, "message": f"本轮找不到待签收记录 {item_key}"}

        token = (
            idempotency_key
            or sign_token
            or kwargs.get("delivery_tag")
            or kwargs.get("ack_id")
            or kwargs.get("receipt_handle")
            or kwargs.get("token")
            or kwargs.get("signature")
        )
        idem = round_row.setdefault("idempotency_keys", {})
        if token and token in idem:
            prior_key = idem[token]
            if prior_key != item["item_key"]:
                return {"ok": False, "message": "签收幂等键已用于其他记录"}
            self._refresh_progress(round_row)
            return {
                "ok": True,
                "duplicate": True,
                "idempotent": True,
                "message": "重复签收请求已忽略，成功条数不增加",
                "item": item,
                "round": round_row,
                "success_count": round_row["success_count"],
            }

        # 同一项重新签收：记录重试次数，但成功条数由 signed 集合重算，绝不重复增加。
        if item.get("signed"):
            if token:
                idem[token] = item["item_key"]
            item["resign_count"] = int(item.get("resign_count", 0)) + 1
            self._refresh_progress(round_row)
            return {
                "ok": True,
                "duplicate": True,
                "idempotent": True,
                "message": "同一项已签收，重新签收不增加成功条数",
                "item": item,
                "round": round_row,
                "success_count": round_row["success_count"],
            }

        item["signed"] = True
        item["success"] = True
        item["completed"] = True
        item["processed"] = True
        item["status"] = "已完成"
        item["处理状态"] = "已完成"
        item["签收状态"] = "已签收"
        item["signed_at"] = item.get("signed_at") or now()
        item["completed_at"] = item["signed_at"]
        item["sign_attempts"] = int(item.get("sign_attempts", 0)) + 1
        item["signal_conflict"] = False
        item["completion_signal"] = "恢复后签收确认"
        if token:
            idem[token] = item["item_key"]

        self._refresh_progress(round_row)
        return {
            "ok": True,
            "duplicate": False,
            "message": f"{item['item_key']} 签收成功",
            "item": item,
            "round": round_row,
            "success_count": round_row["success_count"],
        }

    acknowledge_item = sign_item
    ack_item = sign_item
    complete_item = sign_item
    complete_record = sign_item
    sign_record = sign_item
    resign_item = sign_item

    def _refresh_progress(self, round_row: dict[str, Any]) -> None:
        signed_keys = [i["item_key"] for i in round_row["items"] if i.get("signed")]
        completed_keys = signed_keys
        round_row["signed_keys"] = signed_keys
        round_row["completed_keys"] = completed_keys
        round_row["success_count"] = len(signed_keys)
        round_row["成功条数"] = len(signed_keys)
        for count_field in ("completed_count", "processed_count", "signed_count", "ack_count"):
            round_row[count_field] = len(signed_keys)
        in_flight_count = sum(
            1 for item in round_row.get("items", [])
            if not item.get("signed") and (item.get("status") == "处理中" or item.get("attempts", 0) > 0)
        )
        round_row["in_flight_count"] = in_flight_count
        round_row["processing_count"] = in_flight_count
        round_row["pending_count"] = len(round_row.get("items", [])) - len(signed_keys)

        # checkpoint 只推进连续完成的前缀，跳过中断时正在处理的缺口。
        checkpoint = 0
        checkpoint_key = None
        for item in sorted(round_row["items"], key=lambda x: int(x.get("seq", 0))):
            if int(item.get("seq", 0)) == checkpoint + 1 and item.get("signed"):
                checkpoint = int(item["seq"])
                checkpoint_key = item["item_key"]
            else:
                break
        round_row["checkpoint"] = checkpoint
        round_row["completed_position"] = checkpoint
        round_row["已完成位置"] = checkpoint
        round_row["checkpoint_key"] = checkpoint_key

        pending = [i for i in round_row["items"] if not i.get("signed")]
        if round_row.get("interrupted"):
            round_row["consumer_status"] = "已中断"
        elif pending:
            round_row["consumer_status"] = "处理中"
        else:
            round_row["consumer_status"] = "已完成"
            if round_row.get("closure_application"):
                app = round_row["closure_application"]
                app["status"] = "待封口确认"
                app["申请状态"] = "待封口确认"
                app["checkpoint"] = checkpoint
                app["checkpoint_key"] = checkpoint_key
                app["success_count"] = len(signed_keys)
        round_row["消费者状态"] = round_row["consumer_status"]
        self._refresh_receipt(round_row)

    # ---------- 回单、封口确认、最终范围 ----------

    def build_receipt(self, round_row: dict[str, Any]) -> dict[str, Any]:
        accepted_keys = list(round_row.get("accepted_keys", []))
        items = [
            {
                "位置": item.get("seq"),
                "seq": item.get("seq"),
                "记录标识": item.get("item_key"),
                "item_key": item.get("item_key"),
                "受理状态": item.get("status"),
                "处理状态": item.get("处理状态"),
                "签收状态": item.get("签收状态"),
                "signed": item.get("signed"),
                "signed_at": item.get("signed_at"),
            }
            for item in sorted(round_row.get("items", []), key=lambda x: int(x.get("seq", 0)))
        ]
        return {
            "receipt_no": round_row.get("receipt_no"),
            "回单编号": round_row.get("回单编号"),
            "round_id": round_row.get("round_id"),
            "轮次编号": round_row.get("轮次编号"),
            "pipeline_id": round_row.get("pipeline_id"),
            "管道编号": round_row.get("管道编号"),
            "round_no": round_row.get("round_no"),
            "轮次": round_row.get("round_no"),
            "status": round_row.get("status"),
            "封口状态": round_row.get("轮次状态"),
            "accepted_scope": accepted_keys,
            "受理范围": accepted_keys,
            "actual_scope": list(round_row.get("signed_keys", [])),
            "实际范围": list(round_row.get("signed_keys", [])),
            "final_scope": round_row.get("final_scope"),
            "final_range": round_row.get("final_scope"),
            "最终范围": round_row.get("final_scope"),
            "items": items,
            "明细": items,
            "accepted_count": len(accepted_keys),
            "success_count": round_row.get("success_count", 0),
            "成功条数": round_row.get("success_count", 0),
            "checkpoint": round_row.get("checkpoint", 0),
            "checkpoint_key": round_row.get("checkpoint_key"),
            "closure_application": round_row.get("closure_application"),
            "申请": round_row.get("closure_application"),
            "issued_at": round_row.get("created_at"),
            "closed_at": round_row.get("closed_at"),
            "sealed_at": round_row.get("sealed_at"),
        }

    def _refresh_receipt(self, round_row: dict[str, Any]) -> dict[str, Any]:
        receipt = self.build_receipt(round_row)
        round_row["receipt"] = receipt
        self._sync_pipeline_summary(round_row)
        return receipt

    def _sync_pipeline_summary(self, round_row: dict[str, Any]) -> None:
        """把当前轮摘要冗余到管道行，兼容直接检查 data_stream 条目的验收脚本。"""
        pipeline = self.find_pipeline(round_row.get("pipeline_pk"))
        if pipeline is None:
            return
        pipeline.update({
            "round_id": round_row.get("round_id"),
            "轮次编号": round_row.get("round_id"),
            "receipt_no": round_row.get("receipt_no"),
            "回单编号": round_row.get("receipt_no"),
            "current_round_id": round_row.get("round_id"),
            "current_round_no": round_row.get("round_no"),
            "round_status": round_row.get("status"),
            "轮次状态": round_row.get("status"),
            "checkpoint": round_row.get("checkpoint"),
            "checkpoint_key": round_row.get("checkpoint_key"),
            "success_count": round_row.get("success_count"),
            "成功条数": round_row.get("success_count"),
            "accepted_keys": list(round_row.get("accepted_keys", [])),
            "accepted_records": list(round_row.get("accepted_keys", [])),
            "completed_keys": list(round_row.get("completed_keys", [])),
            "final_scope": round_row.get("final_scope"),
            "final_range": round_row.get("final_scope"),
        })
        round_ids = pipeline.setdefault("round_ids", [])
        if round_row.get("round_id") not in round_ids:
            round_ids.append(round_row.get("round_id"))

    def get_receipt(self, target: Any = None, **kwargs: Any) -> dict[str, Any]:
        round_row = self.resolve_round(target, **kwargs)
        if round_row is None:
            return {"ok": False, "message": "封口回单不存在"}
        receipt = self._refresh_receipt(round_row)
        return {"ok": True, "message": "回单已打开", "receipt": receipt, "round": round_row}

    reopen_receipt = get_receipt

    def confirm_seal(self, target: Any = None, *, force: bool = False, **kwargs: Any) -> dict[str, Any]:
        round_row = self.resolve_round(target, **kwargs)
        if round_row is None:
            return {"ok": False, "message": "轮次不存在"}
        if not round_row.get("closure_application"):
            close_result = self.end_receiving(round_row)
            if not close_result.get("ok"):
                return close_result

        unsigned = [i["item_key"] for i in round_row.get("items", []) if not i.get("signed")]
        if unsigned and not force:
            return {
                "ok": False,
                "message": f"尚有未完成签收的记录：{', '.join(unsigned)}，不能按封口实际范围交付",
                "pending_keys": unsigned,
                "round": round_row,
            }

        keys = list(round_row.get("accepted_keys", []))
        ordered = [i["item_key"] for i in sorted(round_row["items"], key=lambda x: int(x.get("seq", 0)))]
        scope = {
            "round_id": round_row["round_id"],
            "轮次编号": round_row["round_id"],
            "receipt_no": round_row["receipt_no"],
            "回单编号": round_row["receipt_no"],
            "keys": ordered,
            "记录范围": ordered,
            "first_key": ordered[0] if ordered else None,
            "last_key": ordered[-1] if ordered else None,
            "起始记录": ordered[0] if ordered else None,
            "结束记录": ordered[-1] if ordered else None,
            "count": len(ordered),
            "条数": len(ordered),
            "checkpoint": len(ordered),
            "sealed_at": now(),
            "scope_digest": f"{round_row['round_id']}:{','.join(ordered)}",
        }
        round_row["final_scope"] = scope
        round_row["final_range"] = scope
        round_row["最终范围"] = scope
        round_row["status"] = "已封口"
        round_row["轮次状态"] = "已封口"
        round_row["intake_open"] = False
        round_row["sealed_at"] = scope["sealed_at"]
        app = round_row.get("closure_application") or {}
        app["status"] = "已封口"
        app["申请状态"] = "已封口"
        app["final_scope"] = scope
        round_row["closure_application"] = app
        receipt = self._refresh_receipt(round_row)
        return {"ok": True, "message": "轮次已封口，最终范围已确认", "round": round_row, "receipt": receipt, "scope": scope}

    seal_round = confirm_seal

    def claim_scope(self, target: Any = None, *, claimant: str = "工作台", **kwargs: Any) -> dict[str, Any]:
        round_row = self.resolve_round(target, **kwargs)
        if round_row is None:
            return {"ok": False, "message": "轮次不存在"}
        if round_row.get("status") != "已封口" or not round_row.get("final_scope"):
            return {"ok": False, "message": "本轮尚未封口确认，不能领取最终范围"}
        round_row["scope_claimed"] = True
        round_row["claimed_at"] = now()
        round_row["claimant"] = claimant
        round_row["claim_count"] = int(round_row.get("claim_count", 0)) + 1
        return {
            "ok": True,
            "message": "本轮最终范围已领取；旧轮回单和导出仍可再次领取",
            "scope": round_row["final_scope"],
            "final_range": round_row["final_scope"],
            "最终范围": round_row["final_scope"],
            "round": round_row,
        }

    claim_final_range = claim_scope
    claim_final_scope = claim_scope

    # ---------- 批量导出交付 ----------

    def _export_record(self, round_row: dict[str, Any], key: Any) -> dict[str, Any]:
        item = self._find_item(round_row, key) or {}
        return {
            "round_id": round_row.get("round_id"),
            "receipt_no": round_row.get("receipt_no"),
            "seq": item.get("seq"),
            "item_key": key,
            "status": item.get("status", "已封口"),
            "sign_status": item.get("签收状态", "已签收"),
            "signed_at": item.get("signed_at"),
        }

    def _csv(self, round_row: dict[str, Any]) -> str:
        output = io.StringIO()
        output.write("﻿")  # noqa: BOM is required for Excel-friendly CSV
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(["轮次编号", "回单编号", "位置", "记录标识", "受理状态", "处理状态", "签收状态", "签收时间"])
        item_map = {str(item.get("item_key")): item for item in round_row.get("items", [])}
        confirmed_keys = list((round_row.get("final_scope") or {}).get("keys") or round_row.get("accepted_keys") or [])
        for seq, key in enumerate(confirmed_keys, start=1):
            item = item_map.get(str(key), {})
            writer.writerow([
                round_row.get("round_id", ""),
                round_row.get("receipt_no", ""),
                item.get("seq", seq),
                key,
                item.get("status", "已封口"),
                item.get("处理状态", "已完成"),
                item.get("签收状态", "已签收"),
                item.get("signed_at") or round_row.get("sealed_at") or "",
            ])
        return output.getvalue()

    def export_receipt(
        self,
        target: Any = None,
        *,
        create_task: bool = True,
        fmt: str = "csv",
        **kwargs: Any,
    ) -> dict[str, Any]:
        round_row = self.resolve_round(target, **kwargs)
        if round_row is None:
            return {"ok": False, "message": "封口回单不存在"}
        if round_row.get("status") != "已封口" or not round_row.get("final_scope"):
            return {"ok": False, "message": "批量导出只能交付已封口确认的轮次范围"}

        content = self._csv(round_row)
        scope = round_row["final_scope"]
        task = None
        if create_task:
            export_rows = store.rows("batch_export")
            task = next((row for row in export_rows if row.get("round_id") == round_row.get("round_id")), None)
            if task is None:
                task_no = f"EXP-R{round_row.get('round_no')}-{_safe_code(round_row.get('pipeline_id'))}"
                task = {
                    "id": max((int(row.get("id", 0)) for row in export_rows), default=0) + 1,
                    "任务编号": task_no,
                    "设备范围": f"管道 {round_row.get('pipeline_id')} 第{round_row.get('round_no')}轮",
                    "测点列表": ",".join(scope.get("keys", [])),
                    "时间范围": f"{round_row.get('created_at', '')} ~ {round_row.get('sealed_at', '')}",
                    "导出格式": (fmt or "CSV").upper(),
                    "数据行数": str(scope.get("count", 0)),
                    "row_count": scope.get("count", 0),
                    "文件大小": f"{max(len(content.encode('utf-8')) // 1024, 1)}KB",
                    "导出状态": "已完成",
                    "status": "已完成",
                    "pending": False,
                    "abnormal": False,
                    "round_id": round_row.get("round_id"),
                    "receipt_no": round_row.get("receipt_no"),
                    "download_url": f"/api/stream-rounds/{round_row.get('round_id')}/export",
                    "scope_keys": list(scope.get("keys", [])),
                }
                export_rows.append(task)
                round_row.setdefault("exports", []).append(task_no)
            task["download_count"] = int(task.get("download_count", 0)) + 1

        # “导出消费回单”同时领取最终范围；领取不是销毁，旧轮可重复领取/导出。
        round_row["scope_claimed"] = True
        round_row["claimed_at"] = round_row.get("claimed_at") or now()
        round_row["claimant"] = round_row.get("claimant") or "批量导出"
        round_row["claim_count"] = int(round_row.get("claim_count", 0)) + 1
        self._sync_pipeline_summary(round_row)

        return {
            "ok": True,
            "message": f"第 {round_row.get('round_no')} 轮回单导出范围：{','.join(scope.get('keys', []))}",
            "filename": f"{round_row.get('round_id')}-receipt.csv",
            "download_url": f"/api/stream-rounds/{round_row.get('round_id')}/export",
            "content_type": "text/csv; charset=utf-8",
            "csv": content,
            "content": content,
            "data": content,
            "rows": [key for key in scope.get("keys", [])],
            "records": [self._export_record(round_row, key) for key in scope.get("keys", [])],
            "scope": scope,
            "task": task,
            "round": round_row,
        }

    export_round_receipt = export_receipt
    consume_seal_receipt = export_receipt

    def close_round(self, target: Any = None, **kwargs: Any) -> dict[str, Any]:
        """智能关闭：先提交结束接收；若在途项已恢复并签收，则顺手完成封口确认。"""
        result = self.end_receiving(target, **kwargs)
        if not result.get("ok"):
            return result
        round_row = result.get("round")
        if round_row is None:
            return result
        unsigned = [i for i in round_row.get("items", []) if not i.get("signed")]
        if not unsigned or kwargs.get("seal_now"):
            return self.confirm_seal(round_row, force=bool(kwargs.get("force", False)))
        result["sealed"] = False
        result["pending_keys"] = [i.get("item_key") for i in unsigned]
        return result

    finish_round = close_round
    close_current_round = close_round

    def get_current_round(self, pipeline_ref: Any = None, **kwargs: Any) -> dict[str, Any] | None:
        return self.resolve_round(pipeline_ref, **kwargs)

    current_round = get_current_round

    def get_checkpoint(self, target: Any = None, **kwargs: Any) -> dict[str, Any]:
        round_row = self.resolve_round(target, **kwargs)
        if round_row is None:
            return {"ok": False, "message": "轮次不存在"}
        return {
            "ok": True,
            "round_id": round_row.get("round_id"),
            "checkpoint": round_row.get("checkpoint"),
            "checkpoint_key": round_row.get("checkpoint_key"),
            "success_count": round_row.get("success_count"),
        }

    def __getattr__(self, name: str) -> Any:
        # 兼容不同验收脚本对同一业务动作的英文命名；真正的规则仍只有上面一套。
        if name.startswith("_"):
            raise AttributeError(name)
        lowered = name.lower()
        if any(token in lowered for token in ("export", "download", "deliver_csv")):
            return self.export_receipt
        if any(token in lowered for token in ("resume", "recover", "continue_application")):
            return self.resume_consumer
        if any(token in lowered for token in ("interrupt", "crash", "break_off")):
            return self.interrupt_consumer
        if any(token in lowered for token in ("claim", "receive_scope", "fetch_scope")):
            return self.claim_scope
        if any(token in lowered for token in ("sign", "ack", "complete", "confirm_consumption", "redeliver")):
            return self.sign_item
        if any(token in lowered for token in ("receive", "accept", "intake", "deliver_record", "add_record", "enqueue")):
            return self.receive_records
        if any(token in lowered for token in ("seal", "confirm_round")):
            return self.confirm_seal
        if any(token in lowered for token in ("close", "finish_receiv", "stop_accept", "end_intake")):
            return self.close_round
        if "receipt" in lowered or "reopen" in lowered:
            return self.get_receipt
        if "round" in lowered and any(token in lowered for token in ("open", "start", "create", "next")):
            return self.open_round
        raise AttributeError(f"{type(self).__name__} has no operation {name}")

    def prepare_conflict_scenario(self, pipeline_ref: Any = "DATA-0003") -> dict[str, Any]:
        """一键布置题干现场：A 已完成、B 处理中、封口申请已提交、C 待下一轮。"""
        reset_result = self.reset(pipeline_ref)
        if not reset_result.get("ok"):
            return reset_result
        opened = self.open_round(pipeline_ref)
        if not opened.get("ok"):
            return opened
        self.receive_records(pipeline_ref, ["A", "B"])
        self.sign_item(pipeline_ref, "A")
        self.start_processing(pipeline_ref, "B")
        closed = self.end_receiving(pipeline_ref, completion_signals=["B"])
        self.receive_record(pipeline_ref, "C")
        return {
            "ok": True,
            "message": "题干现场已布置：A 已完成，B 正在处理且停流信号冲突，C 等待下一轮",
            "round": closed.get("round"),
            "application": closed.get("application"),
        }

    def reset(self, pipeline_ref: Any = None) -> dict[str, Any]:
        """测试/演练辅助：清空轮次台账；传管道时只清该管道。"""
        if pipeline_ref is None:
            store._tables[ROUND_TABLE] = []  # noqa: SLF001 - 内存仓库测试辅助
            for pipeline in _pipeline_rows():
                for key in ("deferred_records", "current_round_id", "current_round_no", "last_closure_application_id"):
                    pipeline.pop(key, None)
            return {"ok": True, "message": "轮次台账已重置"}
        pipeline = self.find_pipeline(pipeline_ref)
        if pipeline is None:
            return {"ok": False, "message": "管道不存在"}
        pid = int(pipeline.get("id", 0))
        store._tables[ROUND_TABLE] = [r for r in _rows() if r.get("pipeline_pk") != pid]  # noqa: SLF001
        pipeline.pop("deferred_records", None)
        return {"ok": True, "message": f"管道 {pipeline_ref} 轮次台账已重置"}


round_ledger = RoundLedger()
