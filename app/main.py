
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import structlog

from app.core.config import settings
from app.core.database import init_db, close_db
from app.core.redis_client import close_redis
from app.api.telemetry import router as telemetry_router
from app.api.alerts import router as alerts_router
from app.api.websocket import router as ws_router

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    logger.info("starting_up", app=settings.app_name, env=settings.app_env)
    await init_db()
    yield
    logger.info("shutting_down")
    await close_db()
    await close_redis()


app = FastAPI(
    title="Monitoring Agent — Endpoint Telemetry System",
    description=(
        "Production-ready async monitoring backend collecting 10,000+ telemetry "
        "data points every 15 seconds from system sensors. Features AI-powered "
        "alerting via LangChain agents, real-time WebSocket streaming, and "
        "auto-remediation capabilities."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ─── Middleware ────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_timing_middleware(request: Request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    duration_ms = (time.monotonic() - start) * 1000
    response.headers["X-Response-Time-Ms"] = f"{duration_ms:.2f}"
    if duration_ms > 500:
        logger.warning("slow_request", path=request.url.path, duration_ms=round(duration_ms, 2))
    return response


# ─── Exception Handlers ───────────────────────────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error("unhandled_exception", path=request.url.path, error=str(exc))
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "error": str(exc)},
    )


# ─── Routers ──────────────────────────────────────────────────────────────────

app.include_router(telemetry_router)
app.include_router(alerts_router)
app.include_router(ws_router)


# ─── Health & Info Endpoints ──────────────────────────────────────────────────

@app.get("/health", tags=["system"])
async def health():
    """System health check."""
    from app.core.redis_client import get_redis
    checks = {"status": "healthy", "app": settings.app_name}

    try:
        redis = await get_redis()
        await redis.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {e}"
        checks["status"] = "degraded"

    return checks


@app.get("/", tags=["system"])
async def root():
    return {
        "app": settings.app_name,
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": {
            "ingest": "POST /api/v1/telemetry/ingest",
            "batch_ingest": "POST /api/v1/telemetry/ingest/batch",
            "latest_metrics": "GET /api/v1/telemetry/{endpoint_id}/metrics/latest",
            "metric_history": "GET /api/v1/telemetry/{endpoint_id}/metrics",
            "summary": "GET /api/v1/telemetry/{endpoint_id}/summary",
            "list_alerts": "GET /api/v1/alerts/",
            "analyze_alert": "POST /api/v1/alerts/{alert_id}/analyze",
            "ws_stream": "WS /ws/stream/{endpoint_id}",
            "ws_alerts": "WS /ws/alerts",
        },
    }


@app.get("/api/v1/stats", tags=["system"])
async def system_stats():
    """Overall system statistics."""
    from app.core.redis_client import get_redis, RedisCache
    redis = await get_redis()
    cache = RedisCache(redis)
    from app.api.websocket import manager

    endpoint_count = await cache.get_endpoint_count()
    return {
        "active_endpoints": endpoint_count,
        "websocket_connections": manager.stats,
        "ai_provider": settings.ai_provider,
        "ai_model": settings.ai_model,
        "auto_remediation": settings.auto_remediation_enabled,
        "thresholds": {
            "cpu_alert": f"{settings.cpu_alert_threshold}%",
            "memory_alert": f"{settings.memory_alert_threshold}%",
            "disk_alert": f"{settings.disk_alert_threshold}%",
        },
    }