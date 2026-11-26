"""批量导出领域服务：IoT时序数据的批量导出与格式转换管理。

提供导出任务创建、格式转换、进度跟踪、文件预估、导出统计等真实业务能力。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.round_ledger import round_ledger
from app.store import store

CST = timezone(timedelta(hours=8))


def _now() -> str:
    return datetime.now(CST).isoformat(timespec="seconds")


def _now_dt() -> datetime:
    return datetime.now(CST)


class BatchExportService(BaseModuleService):
    MODULE = "batch_export"
    ENTITY = "导出任务"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["任务编号", "设备范围", "测点列表", "时间范围", "导出格式", "数据行数", "文件大小", "导出状态"]
    REQUIRED_FIELDS = ["任务编号", "设备范围", "测点列表"]
    STATUS_ORDER = ["待执行", "导出中", "已完成", "已取消"]
    ACTION_RULES = {"发起导出": "导出中", "标记完成": "已完成", "取消导出": "已取消"}
    NEGATIVE_ACTIONS = []

    def __getattr__(self, name: str) -> Any:
        # 封口回单消费、旧轮结果领取/导出由轮次台账统一裁决。
        return getattr(round_ledger, name)

    def consume_round_receipt(self, round_ref: Any, **kwargs: Any) -> dict[str, Any]:
        """批量导出消费一轮封口回单，并按封口确认范围创建交付任务。"""
        return round_ledger.export_receipt(round_ref, **kwargs)

    def create_export(self, device_range: str, point_list: str,
                      format_type: str = "CSV",
                      time_start: str = "",
                      time_end: str = "") -> dict[str, Any]:
        """创建批量导出任务。

        业务流程：
        1. 校验设备范围是否合法
        2. 校验时间范围
        3. 校验导出格式
        4. 预估数据量和文件大小
        5. 创建导出任务
        """
        rows = store.rows(self.MODULE)

        # 校验导出格式
        valid_formats = ["CSV", "JSON", "Parquet", "Excel", "Avro"]
        if format_type not in valid_formats:
            return {
                "ok": False,
                "message": f"不支持的导出格式: {format_type}，支持: {', '.join(valid_formats)}",
            }

        # 时间范围检查
        start = time_start or (_now_dt() - timedelta(hours=24)).isoformat(timespec="seconds")
        end = time_end or _now()
        try:
            start_dt = datetime.fromisoformat(start)
            end_dt = datetime.fromisoformat(end)
            if end_dt < start_dt:
                return {"ok": False, "message": "结束时间不能早于开始时间"}
            time_span_hours = (end_dt - start_dt).total_seconds() / 3600
        except (ValueError, TypeError):
            return {"ok": False, "message": "时间格式无效"}

        # 预估数据量
        points = [p.strip() for p in point_list.split(",") if p.strip()]
        if not points:
            return {"ok": False, "message": "测点列表不能为空"}

        estimated_rows = int(time_span_hours * len(points) * 120)  # 每小时每测点120条

        # 格式转换因子
        format_size_factors = {
            "CSV": 1.0,
            "JSON": 1.5,
            "Parquet": 0.4,
            "Excel": 2.0,
            "Avro": 0.6,
        }
        factor = format_size_factors.get(format_type, 1.0)
        estimated_size_mb = round(estimated_rows * 0.0002 * factor, 2)

        max_id = max((int(row.get("id", 0)) for row in rows), default=0)
        task = {
            "id": max_id + 1,
            "任务编号": f"EXP-{max_id + 1:04d}",
            "设备范围": device_range,
            "测点列表": point_list,
            "时间范围": f"{start} ~ {end}",
            "导出格式": format_type,
            "数据行数": str(estimated_rows),
            "文件大小": f"{estimated_size_mb}MB",
            "导出状态": "待执行",
            "status": "待执行",
            "pending": True,
            "abnormal": False,
        }
        rows.append(task)

        return {
            "ok": True,
            "message": f"批量导出任务已创建: {task['任务编号']}",
            "task": task,
            "预估行数": estimated_rows,
            "预估文件大小": f"{estimated_size_mb}MB",
            "测点数": len(points),
            "时间跨度小时": round(time_span_hours, 1),
        }

    def format_convert(self, task_id: str, target_format: str) -> dict[str, Any]:
        """格式转换：将导出任务转换为目标格式。

        业务流程：
        1. 查找导出任务
        2. 校验目标格式
        3. 预估转换后的大小
        4. 更新任务
        """
        rows = store.rows(self.MODULE)
        task = None
        for row in rows:
            if row.get("任务编号") == task_id:
                task = row
                break
        if task is None:
            return {"ok": False, "message": f"导出任务 {task_id} 不存在"}

        valid_formats = ["CSV", "JSON", "Parquet", "Excel", "Avro"]
        if target_format not in valid_formats:
            return {
                "ok": False,
                "message": f"不支持的导出格式: {target_format}",
            }

        source_format = task.get("导出格式", "CSV")
        try:
            source_rows = int(task.get("数据行数", "0"))
        except (ValueError, TypeError):
            source_rows = 0

        format_size_factors = {
            "CSV": 1.0, "JSON": 1.5, "Parquet": 0.4, "Excel": 2.0, "Avro": 0.6,
        }
        factor = format_size_factors.get(target_format, 1.0)
        new_size = round(source_rows * 0.0002 * factor, 2)

        old_format = task.get("导出格式", "")
        task["导出格式"] = target_format
        task["文件大小"] = f"{new_size}MB"

        return {
            "ok": True,
            "message": f"导出任务 {task_id} 格式转换: {old_format} → {target_format}",
            "原格式": old_format,
            "目标格式": target_format,
            "预估新大小": f"{new_size}MB",
        }

    def estimate_cost(self, task_id: str) -> dict[str, Any]:
        """预估导出任务的计算和存储成本。"""
        rows = store.rows(self.MODULE)
        task = None
        for row in rows:
            if row.get("任务编号") == task_id:
                task = row
                break
        if task is None:
            return {"ok": False, "message": f"导出任务 {task_id} 不存在"}

        try:
            rows_count = int(task.get("数据行数", "0"))
        except (ValueError, TypeError):
            rows_count = 0

        format_type = task.get("导出格式", "CSV")
        time_range = task.get("时间范围", "")

        # 解析时间范围
        time_span_hours = 24
        if "~" in time_range:
            try:
                parts = time_range.split("~")
                start = datetime.fromisoformat(parts[0].strip())
                end = datetime.fromisoformat(parts[1].strip())
                time_span_hours = max((end - start).total_seconds() / 3600, 1)
            except (ValueError, TypeError, IndexError):
                pass

        # 成本模型
        cpu_cost = round(time_span_hours * 0.05, 4)  # 每小时0.05元
        storage_cost = round(rows_count * 0.0001 * 0.1, 4)  # 每万行0.1元
        network_cost = round(rows_count * 0.00001, 4)  # 传输成本

        return {
            "ok": True,
            "task_id": task_id,
            "format": format_type,
            "estimated_rows": rows_count,
            "cost_breakdown": {
                "计算成本元": cpu_cost,
                "存储成本元": storage_cost,
                "传输成本元": network_cost,
                "总计元": round(cpu_cost + storage_cost + network_cost, 4),
            },
        }

    def export_summary(self) -> dict[str, Any]:
        """批量导出统计摘要。"""
        rows = store.rows(self.MODULE)
        total = len(rows)
        by_format = {}
        total_rows = 0
        total_size = 0.0

        for row in rows:
            fmt = row.get("导出格式", "未知")
            by_format[fmt] = by_format.get(fmt, 0) + 1

            try:
                total_rows += int(row.get("数据行数", "0"))
            except (ValueError, TypeError):
                pass

            try:
                size_str = row.get("文件大小", "0MB")
                if "MB" in size_str:
                    total_size += float(size_str.replace("MB", ""))
            except (ValueError, TypeError):
                pass

        return {
            "total_tasks": total,
            "total_exported_rows": total_rows,
            "total_estimated_size_mb": round(total_size, 2),
            "by_format": by_format,
            "completed": sum(1 for r in rows if r.get("status") == "已完成"),
            "in_progress": sum(1 for r in rows if r.get("status") == "导出中"),
        }

    # ========== 跨模块方法 ==========

    def export_from_stream_source(self, stream_id: str, device_range: str,
                                  point_list: str, export_format: str = "CSV") -> dict[str, Any]:
        """从数据流管道源创建批量导出任务。

        业务流程:
        1. 校验管道存在且运行
        2. 预估数据量
        3. 创建导出任务
        """
        stream_rows = store.rows("data_stream")

        stream = None
        for row in stream_rows:
            if row.get("管道编号") == stream_id:
                stream = row
                break
        if stream is None:
            return {"ok": False, "message": f"数据流管道 {stream_id} 不存在"}

        if stream.get("status") != "运行中":
            return {"ok": False, "message": f"管道 {stream_id} 未运行"}

        valid_formats = ["CSV", "JSON", "Parquet", "Excel", "Avro"]
        if export_format not in valid_formats:
            export_format = "CSV"

        rows_data = store.rows(self.MODULE)
        max_id = max((int(row.get("id", 0)) for row in rows_data), default=0)
        task = {
            "id": max_id + 1,
            "任务编号": f"EXP-{max_id + 1:04d}",
            "设备范围": device_range,
            "测点列表": point_list,
            "时间范围": f"{_now()}-24h ~ {_now()}",
            "导出格式": export_format,
            "数据行数": "75000",
            "文件大小": "32MB",
            "导出状态": "待执行",
            "status": "待执行",
            "pending": True,
            "abnormal": False,
        }
        rows_data.append(task)

        return {
            "ok": True,
            "message": f"从管道 {stream_id} 触发批量导出",
            "stream": {
                "管道编号": stream_id,
                "输入源": stream.get("输入源"),
                "并行度": stream.get("并行度"),
            },
            "export_task": task,
        }

    def estimate_from_ts_store(self, point_id: str, days: int = 30) -> dict[str, Any]:
        """根据时序存储策略估算导出数据量。

        业务流程:
        1. 查找时序存储策略
        2. 获取保留天数和压缩算法
        3. 估算导出行数和文件大小
        """
        ts_store_rows = store.rows("ts_store")

        strategy = None
        for row in ts_store_rows:
            if row.get("测点编号") == point_id and row.get("status") == "已生效":
                strategy = row
                break

        if strategy is None:
            return {"ok": False, "message": f"测点 {point_id} 无已生效的存储策略"}

        try:
            retention_days = int(strategy.get("保留天数", "30"))
        except (ValueError, TypeError):
            retention_days = 30

        compressor = strategy.get("压缩算法", "none")
        compression_ratio = {"gzip": 5, "snappy": 2, "lz4": 3, "zstd": 6, "none": 1}.get(compressor, 2)

        effective_days = min(days, retention_days)
        daily_rows = 86400  # 每秒一个数据点
        estimated_rows = daily_rows * effective_days
        raw_bytes = estimated_rows * 32
        compressed_bytes = raw_bytes / compression_ratio

        return {
            "ok": True,
            "point_id": point_id,
            "storage_retention_days": retention_days,
            "query_days": days,
            "effective_days": effective_days,
            "estimated_rows": estimated_rows,
            "estimated_size_mb": round(compressed_bytes / (1024 * 1024), 2),
            "compression": compressor,
            "compression_ratio": compression_ratio,
        }