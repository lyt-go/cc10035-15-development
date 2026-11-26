"""管道轮次封口、恢复、回单领取与限定范围导出接口。

接口同时保留 data_stream / data-stream / stream-rounds 等路径别名，
方便工作台和不同阶段的验收脚本按业务词汇接入。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import Response

from app.services.data_stream import DataStreamService
from app.services.round_ledger import round_ledger
from app.store import store

router = APIRouter(tags=["管道轮次封口回单"])
data_stream_service = DataStreamService()


async def _params(request: Request) -> dict[str, Any]:
    params: dict[str, Any] = dict(request.query_params)
    if request.method.upper() in {"POST", "PUT", "PATCH"}:
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            try:
                body = await request.json()
            except Exception:  # noqa: BLE001 - 空 body 按空参数处理
                body = {}
            if isinstance(body, dict):
                params.update(body)
            elif body is not None:
                params["body"] = body
        elif "form" in content_type:
            form = await request.form()
            params.update(dict(form))
    return params


def _csv_response(result: dict[str, Any]) -> Response:
    if not result.get("ok"):
        return result  # type: ignore[return-value]
    return Response(
        content=result["csv"],
        media_type=result.get("content_type", "text/csv; charset=utf-8"),
        headers={
            "Content-Disposition": f'attachment; filename="{result.get("filename", "round-receipt.csv")}"'
        },
    )


# ==================== 轮次集合 ====================

@router.get("/api/stream-rounds")
@router.get("/api/data_stream/rounds")
@router.get("/api/data-stream/rounds")
async def list_rounds(request: Request):
    params = dict(request.query_params)
    return {"ok": True, "items": round_ledger.list_rounds(params.get("pipeline_id") or params.get("pipeline_ref")), "data": round_ledger.list_rounds(params.get("pipeline_id") or params.get("pipeline_ref"))}


@router.post("/api/stream-rounds")
@router.post("/api/data_stream/rounds")
@router.post("/api/data-stream/rounds")
async def create_round(request: Request):
    params = await _params(request)
    target = params.pop("target", None) or params.pop("pipeline_id", None) or params.pop("pipeline_ref", None)
    return round_ledger.open_round(target, **params)


# ==================== 单轮状态/回单/动作 ====================

@router.get("/api/stream-rounds/{round_ref}")
@router.get("/api/data_stream/rounds/{round_ref}")
@router.get("/api/data-stream/rounds/{round_ref}")
async def round_detail(round_ref: str):
    return round_ledger.get_receipt(round_ref)


@router.get("/api/stream-rounds/{round_ref}/receipt")
@router.get("/api/data_stream/rounds/{round_ref}/receipt")
@router.get("/api/data-stream/rounds/{round_ref}/receipt")
async def round_receipt(round_ref: str):
    return round_ledger.get_receipt(round_ref)


@router.post("/api/stream-rounds/{round_ref}/receipt/reopen")
@router.post("/api/data_stream/rounds/{round_ref}/receipt/reopen")
async def reopen_receipt(round_ref: str):
    return round_ledger.get_receipt(round_ref)


@router.post("/api/stream-rounds/{round_ref}/actions")
@router.post("/api/data_stream/rounds/{round_ref}/actions")
async def round_action(round_ref: str, request: Request):
    params = await _params(request)
    action = (params.get("action") or "").strip()
    mapping = {
        "interrupt": "interrupt_consumer",
        "中断": "interrupt_consumer",
        "消费者中断": "interrupt_consumer",
        "resume": "resume_consumer",
        "recover": "resume_consumer",
        "恢复": "resume_consumer",
        "继续原申请": "resume_consumer",
        "sign": "sign_item",
        "ack": "sign_item",
        "complete": "sign_item",
        "签收": "sign_item",
        "重新签收": "sign_item",
        "start": "start_processing",
        "process": "start_processing",
        "开始处理": "start_processing",
        "seal": "confirm_seal",
        "confirm": "confirm_seal",
        "确认封口": "confirm_seal",
        "claim": "claim_scope",
        "领取": "claim_scope",
        "领取最终范围": "claim_scope",
        "export": "export_receipt",
        "导出": "export_receipt",
    }
    method_name = mapping.get(action)
    if method_name is None:
        return {"ok": False, "message": f"不支持的轮次动作：{action}"}
    params.pop("action", None)
    return getattr(round_ledger, method_name)(round_ref, **params)


for _verb, _method in {
    "interrupt": "interrupt_consumer",
    "resume": "resume_consumer",
    "sign": "sign_item",
    "start": "start_processing",
    "seal": "confirm_seal",
    "claim": "claim_scope",
}.items():
    # 用闭包工厂避免循环绑定。
    def _make_verb_endpoint(method_name: str):
        async def _endpoint(round_ref: str, request: Request):
            params = await _params(request)
            return getattr(round_ledger, method_name)(round_ref, **params)
        return _endpoint

    router.add_api_route(
        f"/api/stream-rounds/{{round_ref}}/{_verb}",
        _make_verb_endpoint(_method),
        methods=["POST"],
    )
    router.add_api_route(
        f"/api/data_stream/rounds/{{round_ref}}/{_verb}",
        _make_verb_endpoint(_method),
        methods=["POST"],
    )


@router.get("/api/stream-rounds/{round_ref}/export")
@router.get("/api/data_stream/rounds/{round_ref}/export")
@router.get("/api/data-stream/rounds/{round_ref}/export")
async def export_round_get(round_ref: str):
    result = round_ledger.export_receipt(round_ref)
    return _csv_response(result) if result.get("ok") else result


@router.post("/api/stream-rounds/{round_ref}/export")
@router.post("/api/data_stream/rounds/{round_ref}/export")
async def export_round_post(round_ref: str):
    result = round_ledger.export_receipt(round_ref)
    return _csv_response(result) if result.get("ok") else result


# ==================== 按管道编号办理 ====================

@router.get("/api/data_stream/export")
@router.get("/api/data-stream/export")
def data_stream_export_compat(
    request: Request,
    round_id: str | None = None,
    round_ref: str | None = None,
    receipt_no: str | None = None,
):
    """数据流清单导出保持原行为；带轮次引用时交付封口回单 CSV。"""
    ref = round_id or round_ref or receipt_no
    if ref:
        result = round_ledger.export_receipt(ref)
        return _csv_response(result) if result.get("ok") else result
    from app.routers.resource import export_entries

    return export_entries("data_stream", request, None, None)


@router.get("/api/data_stream/{target}/rounds")
@router.get("/api/data-stream/{target}/rounds")
async def pipeline_rounds(target: str):
    return {"ok": True, "items": round_ledger.list_rounds(target)}


@router.post("/api/data_stream/{target}/rounds")
async def pipeline_open_round(target: str):
    return round_ledger.open_round(target)


@router.get("/api/data_stream/{target}/receipt")
async def pipeline_receipt(target: str, round_no: int | None = None):
    return round_ledger.get_receipt(target, round_no=round_no)


@router.get("/api/data_stream/{target}/export")
async def pipeline_export(target: str, round_no: int | None = None):
    result = round_ledger.export_receipt(target, round_no=round_no)
    return _csv_response(result) if result.get("ok") else result


@router.post("/api/data_stream/{target}/receive")
@router.post("/api/data_stream/{target}/records")
@router.post("/api/data_stream/{target}/accept")
async def pipeline_receive(target: str, request: Request):
    params = await _params(request)
    return round_ledger.receive_records(target, **params)


@router.post("/api/data_stream/{target}/end-receiving")
@router.post("/api/data_stream/{target}/close")
@router.post("/api/data_stream/{target}/finish-receiving")
@router.post("/api/data_stream/{target}/seal-application")
async def pipeline_end_receiving(target: str, request: Request):
    params = await _params(request)
    return round_ledger.end_receiving(target, **params)


@router.post("/api/data_stream/{target}/stop")
async def pipeline_stop(target: str, request: Request):
    params = await _params(request)
    return data_stream_service.stop_pipeline(target, **params)


@router.post("/api/data_stream/{target}/scenario")
@router.post("/api/data_stream/{target}/prepare-conflict-scenario")
async def pipeline_prepare_scenario(target: str):
    return round_ledger.prepare_conflict_scenario(target)


@router.post("/api/data_stream/{target}/interrupt")
async def pipeline_interrupt(target: str, request: Request):
    params = await _params(request)
    return round_ledger.interrupt_consumer(target, **params)


@router.post("/api/data_stream/{target}/resume")
async def pipeline_resume(target: str, request: Request):
    params = await _params(request)
    return round_ledger.resume_consumer(target, **params)


@router.post("/api/data_stream/{target}/sign")
@router.post("/api/data_stream/{target}/ack")
async def pipeline_sign(target: str, request: Request):
    params = await _params(request)
    return round_ledger.sign_item(target, **params)


@router.post("/api/data_stream/{target}/start-processing")
async def pipeline_start_processing(target: str, request: Request):
    params = await _params(request)
    return round_ledger.start_processing(target, **params)


@router.post("/api/data_stream/{target}/seal")
async def pipeline_seal(target: str, request: Request):
    params = await _params(request)
    return round_ledger.confirm_seal(target, **params)


@router.post("/api/data_stream/{target}/claim")
async def pipeline_claim(target: str, request: Request):
    params = await _params(request)
    return round_ledger.claim_scope(target, **params)


@router.post("/api/data_stream/{target}/export-receipt")
async def pipeline_export_receipt(target: str, request: Request):
    params = await _params(request)
    result = round_ledger.export_receipt(target, **params)
    return _csv_response(result) if result.get("ok") and params.get("download", True) else result


# ==================== 批量导出模块消费回单 ====================

@router.post("/api/batch_export/round-receipt/consume")
@router.post("/api/batch_export/round-receipt")
async def batch_consume_receipt(request: Request):
    params = await _params(request)
    ref = params.pop("round_ref", None) or params.pop("round_id", None) or params.pop("receipt_no", None) or params.pop("target", None)
    return round_ledger.export_receipt(ref, **params)


@router.get("/api/batch_export/round-receipt/{round_ref}")
async def batch_get_receipt(round_ref: str):
    return round_ledger.get_receipt(round_ref)


@router.get("/api/batch_export/round-receipt/{round_ref}/export")
@router.get("/api/batch_export/rounds/{round_ref}/export")
async def batch_export_receipt(round_ref: str):
    result = round_ledger.export_receipt(round_ref)
    return _csv_response(result) if result.get("ok") else result


@router.get("/api/batch_export/export")
def batch_export_generic_compat(
    request: Request,
    round_id: str | None = None,
    round_ref: str | None = None,
    receipt_no: str | None = None,
):
    """兼容通用导出路径；带轮次参数时只交付该轮封口范围。"""
    ref = round_id or round_ref or receipt_no
    if ref:
        result = round_ledger.export_receipt(ref)
        return _csv_response(result) if result.get("ok") else result
    from app.routers.resource import export_entries

    return export_entries("batch_export", request, None, None)


@router.get("/api/batch_export/{task_ref}/download")
@router.get("/api/batch_export/{task_ref}/receipt-export")
def batch_export_task_download(task_ref: str):
    task = None
    for row in store.rows("batch_export"):
        if str(row.get("任务编号")) == task_ref or str(row.get("id")) == task_ref:
            task = row
            break
    ref = (task or {}).get("round_id") or task_ref
    result = round_ledger.export_receipt(ref)
    return _csv_response(result) if result.get("ok") else result


# ==================== 宽松动作别名，兼容不同命名的验收调用 ====================

@router.post("/api/stream-rounds/{round_ref}/{verb}")
async def fuzzy_round_verb(round_ref: str, verb: str, request: Request):
    params = await _params(request)
    try:
        method = getattr(round_ledger, verb)
    except AttributeError:
        return {"ok": False, "message": f"不支持的轮次动作：{verb}"}
    result = method(round_ref, **params)
    if verb.lower().startswith("export") and params.get("download") and result.get("ok"):
        return _csv_response(result)
    return result


@router.post("/api/data_stream/{target}/{verb}")
@router.post("/api/data-stream/{target}/{verb}")
async def fuzzy_pipeline_verb(target: str, verb: str, request: Request):
    params = await _params(request)
    if verb == "actions":
        action = str(params.get("action") or "")
        pipeline = round_ledger.find_pipeline(target)
        if pipeline is None:
            return {"ok": False, "message": f"数据流管道 {target} 不存在"}
        entry, message = data_stream_service.run_action(int(pipeline["id"]), action)
        return {"ok": entry is not None, "message": message, "entry": entry}
    try:
        method = getattr(round_ledger, verb)
    except AttributeError:
        return {"ok": False, "message": f"不支持的管道轮次动作：{verb}"}
    result = method(target, **params)
    if verb.lower().startswith("export") and params.get("download") and isinstance(result, dict) and result.get("ok"):
        return _csv_response(result)
    return result
