from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from typing import Optional, List
import structlog

from app.core.database import get_db
from app.core.redis_client import get_redis, RedisCache
from app.models.telemetry import TelemetryPayload, TelemetryBatch, EndpointResponse, MetricSummary
from app.services.telemetry_service import TelemetryService
from app.services.alert_service import AlertService

router = APIRouter(prefix="/api/v1/telemetry", tags=["telemetry"])
logger = structlog.get_logger()


async def get_services(db: AsyncSession = Depends(get_db)):
    redis = await get_redis()
    cache = RedisCache(redis)
    return TelemetryService(db, cache), AlertService(db, cache)


@router.post("/ingest", summary="Ingest single telemetry payload")
async def ingest(
    payload: TelemetryPayload,
    services=Depends(get_services),
):
    """
    Ingest a single telemetry snapshot from an endpoint.
    Stores to PostgreSQL, caches in Redis, and evaluates alert thresholds.
    """
    telemetry_svc, alert_svc = services
    metric = await telemetry_svc.ingest(payload)

    # Evaluate thresholds and trigger alerts
    alerts = await alert_svc.evaluate_telemetry(payload)

    return {
        "status": "ok",
        "metric_id": metric.id,
        "alerts_triggered": len(alerts),
        "alert_ids": [a.id for a in alerts],
    }


@router.post("/ingest/batch", summary="Ingest batch of telemetry payloads")
async def ingest_batch(
    batch: TelemetryBatch,
    services=Depends(get_services),
):
    """
    Bulk ingest up to 500 metric snapshots in one request.
    Ideal for buffered/offline scenarios.
    """
    telemetry_svc, alert_svc = services
    total_alerts = 0

    count = await telemetry_svc.ingest_batch(batch.metrics)

    for payload in batch.metrics:
        alerts = await alert_svc.evaluate_telemetry(payload)
        total_alerts += len(alerts)

    return {
        "status": "ok",
        "metrics_ingested": count,
        "total_alerts_triggered": total_alerts,
    }


@router.get("/{endpoint_id}/metrics/latest", summary="Get latest metrics for endpoint")
async def get_latest(endpoint_id: str, services=Depends(get_services)):
    telemetry_svc, _ = services
    data = await telemetry_svc.get_latest_metrics(endpoint_id)
    if not data:
        raise HTTPException(404, f"No metrics found for endpoint {endpoint_id}")
    return data


@router.get("/{endpoint_id}/metrics", summary="Query historical metrics")
async def get_metrics_range(
    endpoint_id: str,
    start: Optional[datetime] = Query(default=None),
    end: Optional[datetime] = Query(default=None),
    limit: int = Query(default=500, le=5000),
    services=Depends(get_services),
):
    end = end or datetime.utcnow()
    start = start or (end - timedelta(hours=1))
    telemetry_svc, _ = services
    metrics = await telemetry_svc.get_metrics_range(endpoint_id, start, end, limit)
    return {"endpoint_id": endpoint_id, "count": len(metrics), "metrics": metrics}


@router.get("/{endpoint_id}/summary", response_model=MetricSummary, summary="Aggregated stats summary")
async def get_summary(
    endpoint_id: str,
    hours: int = Query(default=1, ge=1, le=168),
    services=Depends(get_services),
):
    telemetry_svc, _ = services
    summary = await telemetry_svc.get_summary(endpoint_id, hours=hours)
    if not summary:
        raise HTTPException(404, f"No data for endpoint {endpoint_id} in last {hours}h")
    return summary