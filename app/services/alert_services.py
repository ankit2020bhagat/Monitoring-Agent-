
import json
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, update
import structlog

from app.models.alerts import AlertModel, AlertCreate, AlertResponse, AlertSeverity, AlertStatus, AlertType
from app.models.telemetry import TelemetryPayload
from app.core.redis_client import RedisCache
from app.core.config import settings
from app.agents.monitoring_agent import get_monitoring_agent

logger = structlog.get_logger()


class AlertService:
    def __init__(self, db: AsyncSession, cache: RedisCache):
        self.db = db
        self.cache = cache

    # ─── Threshold Evaluation ─────────────────────────────────────────────────

    async def evaluate_telemetry(self, payload: TelemetryPayload) -> List[AlertModel]:
        """Check incoming metrics against thresholds, create alerts if needed."""
        triggered = []

        checks = [
            (
                payload.cpu.percent > settings.cpu_alert_threshold,
                AlertType.CPU_HIGH,
                AlertSeverity.CRITICAL if payload.cpu.percent > 95 else AlertSeverity.HIGH,
                f"CPU usage at {payload.cpu.percent:.1f}%",
                f"CPU has exceeded {settings.cpu_alert_threshold}% threshold (current: {payload.cpu.percent:.1f}%)",
            ),
            (
                payload.memory.percent > settings.memory_alert_threshold,
                AlertType.MEMORY_HIGH,
                AlertSeverity.CRITICAL if payload.memory.percent > 95 else AlertSeverity.HIGH,
                f"Memory usage at {payload.memory.percent:.1f}%",
                f"Memory consumption at {payload.memory.percent:.1f}% — risk of OOM",
            ),
            (
                payload.disk.percent > settings.disk_alert_threshold,
                AlertType.DISK_HIGH,
                AlertSeverity.HIGH,
                f"Disk usage at {payload.disk.percent:.1f}%",
                f"Disk at {payload.disk.percent:.1f}% — low free space on primary partition",
            ),
            (
                (payload.network.errors_in + payload.network.errors_out) > settings.network_error_threshold,
                AlertType.NETWORK_ERRORS,
                AlertSeverity.MEDIUM,
                "Elevated network errors detected",
                f"Network errors: {payload.network.errors_in} in / {payload.network.errors_out} out",
            ),
        ]

        for condition, alert_type, severity, title, description in checks:
            if condition:
                alert = await self._create_alert_if_not_cooling(
                    endpoint_id=payload.endpoint_id,
                    alert_type=alert_type,
                    severity=severity,
                    title=title,
                    description=description,
                    metric_snapshot=payload.model_dump(mode="json"),
                )
                if alert:
                    triggered.append(alert)

        return triggered

    # ─── Alert CRUD ───────────────────────────────────────────────────────────

    async def create_alert(self, data: AlertCreate) -> AlertModel:
        alert = AlertModel(
            endpoint_id=data.endpoint_id,
            alert_type=data.alert_type.value,
            severity=data.severity.value,
            title=data.title,
            description=data.description,
            metric_snapshot=data.metric_snapshot,
        )
        self.db.add(alert)
        await self.db.flush()
        logger.info("alert_created", alert_id=alert.id, type=alert.alert_type, severity=alert.severity)

        # Publish to Redis channel for WS subscribers
        await self.cache.publish_alert("alerts", {
            "alert_id": alert.id,
            "endpoint_id": alert.endpoint_id,
            "type": alert.alert_type,
            "severity": alert.severity,
            "title": alert.title,
            "triggered_at": alert.triggered_at.isoformat(),
        })
        return alert

    async def get_alert(self, alert_id: str) -> Optional[AlertModel]:
        result = await self.db.execute(select(AlertModel).where(AlertModel.id == alert_id))
        return result.scalar_one_or_none()

    async def list_alerts(
        self,
        endpoint_id: Optional[str] = None,
        status: Optional[str] = None,
        severity: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[AlertModel]:
        q = select(AlertModel)
        conditions = []
        if endpoint_id:
            conditions.append(AlertModel.endpoint_id == endpoint_id)
        if status:
            conditions.append(AlertModel.status == status)
        if severity:
            conditions.append(AlertModel.severity == severity)
        if conditions:
            q = q.where(and_(*conditions))
        q = q.order_by(AlertModel.triggered_at.desc()).limit(limit).offset(offset)
        result = await self.db.execute(q)
        return result.scalars().all()

    async def resolve_alert(self, alert_id: str, summary: Optional[str] = None) -> Optional[AlertModel]:
        alert = await self.get_alert(alert_id)
        if not alert:
            return None
        alert.status = AlertStatus.RESOLVED.value
        alert.resolved_at = datetime.utcnow()
        if summary:
            alert.resolution_summary = summary
        return alert

    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> Optional[AlertModel]:
        alert = await self.get_alert(alert_id)
        if not alert:
            return None
        alert.acknowledged_at = datetime.utcnow()
        alert.acknowledged_by = acknowledged_by
        if alert.status == AlertStatus.OPEN.value:
            alert.status = AlertStatus.INVESTIGATING.value
        return alert

    # ─── AI Analysis Pipeline ─────────────────────────────────────────────────

    async def run_ai_analysis(self, alert_id: str, context: Optional[str] = None) -> Dict[str, Any]:
        """
        Trigger LangChain agent to analyze an alert.
        Updates the alert record with findings.
        """
        alert = await self.get_alert(alert_id)
        if not alert:
            return {"error": "Alert not found"}

        alert.status = AlertStatus.INVESTIGATING.value
        await self.db.flush()

        try:
            agent = get_monitoring_agent()
            result = await agent.analyze_alert(
                alert_id=alert_id,
                endpoint_id=alert.endpoint_id,
                alert_type=alert.alert_type,
                metrics=alert.metric_snapshot or {},
                context=context,
            )

            alert.ai_analysis = result.get("analysis", "")
            alert.root_cause = result.get("root_cause", "")
            alert.remediation_actions = result.get("actions_taken", [])

            if result.get("escalation_needed"):
                alert.status = AlertStatus.ESCALATED.value
                logger.warning("alert_escalated", alert_id=alert_id)
            elif result.get("actions_taken"):
                alert.status = AlertStatus.AUTO_RESOLVED.value
                alert.resolved_at = datetime.utcnow()
                alert.resolution_summary = f"Auto-resolved by AI agent. {len(result['actions_taken'])} action(s) taken."
            else:
                alert.status = AlertStatus.OPEN.value

            await self.db.flush()
            return result

        except Exception as e:
            logger.error("ai_analysis_error", alert_id=alert_id, error=str(e))
            alert.status = AlertStatus.ESCALATED.value
            alert.ai_analysis = f"Analysis failed: {str(e)}"
            await self.db.flush()
            return {"error": str(e), "escalation_needed": True}

    # ─── Internals ────────────────────────────────────────────────────────────

    async def _create_alert_if_not_cooling(
        self, endpoint_id, alert_type, severity, title, description, metric_snapshot
    ) -> Optional[AlertModel]:
        """Deduplicate alerts using Redis cooldown window."""
        cooldown_key = f"{endpoint_id}:{alert_type.value}"
        existing = await self.cache.get_alert_state(cooldown_key)
        if existing:
            logger.debug("alert_suppressed_cooldown", key=cooldown_key)
            return None

        alert = await self.create_alert(AlertCreate(
            endpoint_id=endpoint_id,
            alert_type=alert_type,
            severity=severity,
            title=title,
            description=description,
            metric_snapshot=metric_snapshot,
        ))

        # Set cooldown
        await self.cache.set_alert_state(
            cooldown_key,
            {"alert_id": alert.id, "created_at": datetime.utcnow().isoformat()},
            ttl=settings.alert_cooldown_seconds,
        )

        # Trigger AI analysis in background if critical
        if severity in (AlertSeverity.CRITICAL, AlertSeverity.HIGH) and settings.auto_remediation_enabled:
            import asyncio
            asyncio.create_task(self._background_analysis(alert.id))

        return alert

    async def _background_analysis(self, alert_id: str):
        """Run AI analysis as a background task."""
        try:
            from app.core.database import AsyncSessionLocal
            from app.core.redis_client import get_redis
            async with AsyncSessionLocal() as db:
                redis = await get_redis()
                from app.core.redis_client import RedisCache
                svc = AlertService(db, RedisCache(redis))
                await svc.run_ai_analysis(alert_id)
                await db.commit()
        except Exception as e:
            logger.error("background_analysis_failed", alert_id=alert_id, error=str(e))