from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
import structlog

from app.core.database import get_db
from app.core.redis_client import get_redis, RedisCache
from app.models.alerts import AlertCreate, AlertResponse, AlertAnalysisRequest
from app.services.alert_service import AlertService

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])
logger = structlog.get_logger()


async def get_alert_service(db: AsyncSession = Depends(get_db)) -> AlertService:
    redis = await get_redis()
    return AlertService(db, RedisCache(redis))


@router.get("/", response_model=List[AlertResponse], summary="List alerts")
async def list_alerts(
    endpoint_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0),
    svc: AlertService = Depends(get_alert_service),
):
    alerts = await svc.list_alerts(endpoint_id, status, severity, limit, offset)
    return alerts


@router.post("/", response_model=AlertResponse, summary="Create alert manually")
async def create_alert(
    data: AlertCreate,
    svc: AlertService = Depends(get_alert_service),
):
    alert = await svc.create_alert(data)
    return alert


@router.get("/{alert_id}", response_model=AlertResponse, summary="Get alert by ID")
async def get_alert(alert_id: str, svc: AlertService = Depends(get_alert_service)):
    alert = await svc.get_alert(alert_id)
    if not alert:
        raise HTTPException(404, "Alert not found")
    return alert


@router.post("/{alert_id}/resolve", response_model=AlertResponse, summary="Resolve an alert")
async def resolve_alert(
    alert_id: str,
    summary: Optional[str] = Query(None),
    svc: AlertService = Depends(get_alert_service),
):
    alert = await svc.resolve_alert(alert_id, summary)
    if not alert:
        raise HTTPException(404, "Alert not found")
    return alert


@router.post("/{alert_id}/acknowledge", response_model=AlertResponse, summary="Acknowledge an alert")
async def acknowledge_alert(
    alert_id: str,
    acknowledged_by: str = Query(...),
    svc: AlertService = Depends(get_alert_service),
):
    alert = await svc.acknowledge_alert(alert_id, acknowledged_by)
    if not alert:
        raise HTTPException(404, "Alert not found")
    return alert


@router.post("/{alert_id}/analyze", summary="Trigger AI analysis on an alert")
async def analyze_alert(
    alert_id: str,
    background_tasks: BackgroundTasks,
    context: Optional[str] = Query(None, description="Additional context for the AI"),
    async_mode: bool = Query(default=True, description="Run analysis in background"),
    svc: AlertService = Depends(get_alert_service),
):
    """
    Trigger the LangChain monitoring agent to analyze this alert.
    The agent will run diagnostics, determine root cause, and attempt auto-remediation.
    """
    alert = await svc.get_alert(alert_id)
    if not alert:
        raise HTTPException(404, "Alert not found")

    if async_mode:
        background_tasks.add_task(svc.run_ai_analysis, alert_id, context)
        return {"status": "analysis_queued", "alert_id": alert_id}

    result = await svc.run_ai_analysis(alert_id, context)
    return result


@router.post("/analyze/endpoint", summary="Analyze current endpoint state with AI")
async def analyze_endpoint(
    request: AlertAnalysisRequest,
    svc: AlertService = Depends(get_alert_service),
):
    """Run a proactive AI analysis on an endpoint's current metrics."""
    from app.agents.monitoring_agent import get_monitoring_agent
    from app.core.redis_client import get_redis, RedisCache

    redis = await get_redis()
    cache = RedisCache(redis)
    latest = await cache.get_metrics(request.endpoint_id)

    if not latest:
        raise HTTPException(404, f"No recent metrics for endpoint {request.endpoint_id}")

    agent = get_monitoring_agent()
    result = await agent.analyze_anomaly(
        endpoint_id=request.endpoint_id,
        current_metrics=latest,
    )
    return result