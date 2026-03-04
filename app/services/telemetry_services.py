
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
import structlog

from app.models.telemetry import (
    TelemetryPayload, MetricModel, EndpointModel, MetricSummary
)
from app.core.redis_client import RedisCache
from app.core.config import settings

logger = structlog.get_logger()


class TelemetryService:
    def __init__(self, db: AsyncSession, cache: RedisCache):
        self.db = db
        self.cache = cache

    async def ingest(self, payload: TelemetryPayload) -> MetricModel:
        """Store a single telemetry payload to DB and update Redis cache."""
        # Upsert endpoint record
        await self._upsert_endpoint(payload)

        # Persist metric to PostgreSQL
        metric = MetricModel(
            endpoint_id=payload.endpoint_id,
            collected_at=payload.collected_at,
            cpu_percent=payload.cpu.percent,
            cpu_count=payload.cpu.count,
            cpu_freq_mhz=payload.cpu.freq_mhz,
            load_avg_1m=payload.cpu.load_avg_1m,
            load_avg_5m=payload.cpu.load_avg_5m,
            load_avg_15m=payload.cpu.load_avg_15m,
            memory_total_gb=payload.memory.total_gb,
            memory_used_gb=payload.memory.used_gb,
            memory_percent=payload.memory.percent,
            swap_percent=payload.memory.swap_percent,
            disk_total_gb=payload.disk.total_gb,
            disk_used_gb=payload.disk.used_gb,
            disk_percent=payload.disk.percent,
            disk_read_mb=payload.disk.read_mb,
            disk_write_mb=payload.disk.write_mb,
            net_bytes_sent_mb=payload.network.bytes_sent_mb,
            net_bytes_recv_mb=payload.network.bytes_recv_mb,
            net_packets_sent=payload.network.packets_sent,
            net_packets_recv=payload.network.packets_recv,
            net_errors_in=payload.network.errors_in,
            net_errors_out=payload.network.errors_out,
            process_count=payload.process_count,
            top_processes=[p.model_dump() for p in payload.top_processes],
        )
        self.db.add(metric)
        await self.db.flush()

        # Update Redis cache (non-blocking)
        metrics_dict = payload.model_dump(mode="json")
        await self.cache.set_metrics(payload.endpoint_id, metrics_dict)
        await self.cache.push_metric_stream(payload.endpoint_id, metrics_dict)

        logger.debug(
            "metric_ingested",
            endpoint_id=payload.endpoint_id,
            cpu=payload.cpu.percent,
            memory=payload.memory.percent,
        )
        return metric

    async def ingest_batch(self, payloads: List[TelemetryPayload]) -> int:
        """Bulk ingest multiple metrics. Returns count ingested."""
        count = 0
        for payload in payloads:
            await self.ingest(payload)
            count += 1
        logger.info("batch_ingested", count=count)
        return count

    async def get_latest_metrics(self, endpoint_id: str) -> Optional[Dict]:
        """Get latest metrics from Redis cache or DB fallback."""
        cached = await self.cache.get_metrics(endpoint_id)
        if cached:
            return cached

        # DB fallback
        result = await self.db.execute(
            select(MetricModel)
            .where(MetricModel.endpoint_id == endpoint_id)
            .order_by(MetricModel.collected_at.desc())
            .limit(1)
        )
        metric = result.scalar_one_or_none()
        return self._metric_to_dict(metric) if metric else None

    async def get_metrics_range(
        self,
        endpoint_id: str,
        start: datetime,
        end: datetime,
        limit: int = 1000,
    ) -> List[Dict]:
        result = await self.db.execute(
            select(MetricModel)
            .where(
                and_(
                    MetricModel.endpoint_id == endpoint_id,
                    MetricModel.collected_at >= start,
                    MetricModel.collected_at <= end,
                )
            )
            .order_by(MetricModel.collected_at.desc())
            .limit(limit)
        )
        return [self._metric_to_dict(m) for m in result.scalars().all()]

    async def get_summary(
        self, endpoint_id: str, hours: int = 1
    ) -> Optional[MetricSummary]:
        end = datetime.utcnow()
        start = end - timedelta(hours=hours)

        result = await self.db.execute(
            select(
                func.avg(MetricModel.cpu_percent).label("avg_cpu"),
                func.max(MetricModel.cpu_percent).label("max_cpu"),
                func.avg(MetricModel.memory_percent).label("avg_mem"),
                func.max(MetricModel.memory_percent).label("max_mem"),
                func.avg(MetricModel.disk_percent).label("avg_disk"),
                func.count(MetricModel.id).label("count"),
            ).where(
                and_(
                    MetricModel.endpoint_id == endpoint_id,
                    MetricModel.collected_at.between(start, end),
                )
            )
        )
        row = result.one_or_none()
        if not row or not row.count:
            return None

        return MetricSummary(
            endpoint_id=endpoint_id,
            period_start=start,
            period_end=end,
            avg_cpu_percent=round(row.avg_cpu or 0, 2),
            max_cpu_percent=round(row.max_cpu or 0, 2),
            avg_memory_percent=round(row.avg_mem or 0, 2),
            max_memory_percent=round(row.max_mem or 0, 2),
            avg_disk_percent=round(row.avg_disk or 0, 2),
            data_points=row.count,
        )

    async def _upsert_endpoint(self, payload: TelemetryPayload):
        result = await self.db.execute(
            select(EndpointModel).where(EndpointModel.id == payload.endpoint_id)
        )
        endpoint = result.scalar_one_or_none()

        if endpoint:
            endpoint.last_seen = datetime.utcnow()
            endpoint.hostname = payload.hostname
            endpoint.ip_address = payload.ip_address
        else:
            endpoint = EndpointModel(
                id=payload.endpoint_id,
                hostname=payload.hostname,
                ip_address=payload.ip_address,
                os_info=payload.os_info,
                tags=payload.tags,
            )
            self.db.add(endpoint)

    def _metric_to_dict(self, m: MetricModel) -> Dict:
        return {
            "id": m.id,
            "endpoint_id": m.endpoint_id,
            "collected_at": m.collected_at.isoformat(),
            "cpu": {
                "percent": m.cpu_percent,
                "count": m.cpu_count,
                "freq_mhz": m.cpu_freq_mhz,
                "load_avg": {
                    "1m": m.load_avg_1m,
                    "5m": m.load_avg_5m,
                    "15m": m.load_avg_15m,
                },
            },
            "memory": {
                "total_gb": m.memory_total_gb,
                "used_gb": m.memory_used_gb,
                "percent": m.memory_percent,
                "swap_percent": m.swap_percent,
            },
            "disk": {
                "total_gb": m.disk_total_gb,
                "used_gb": m.disk_used_gb,
                "percent": m.disk_percent,
                "read_mb": m.disk_read_mb,
                "write_mb": m.disk_write_mb,
            },
            "network": {
                "bytes_sent_mb": m.net_bytes_sent_mb,
                "bytes_recv_mb": m.net_bytes_recv_mb,
                "errors_in": m.net_errors_in,
                "errors_out": m.net_errors_out,
            },
            "process_count": m.process_count,
        }