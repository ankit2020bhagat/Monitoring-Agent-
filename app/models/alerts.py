from sqlalchemy import Column, String, Float, Integer, DateTime, JSON, Text, Enum
from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from enum import Enum as PyEnum
import uuid

from app.core.database import Base


class AlertSeverity(str, PyEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertStatus(str, PyEnum):
    OPEN = "open"
    INVESTIGATING = "investigating"
    AUTO_RESOLVED = "auto_resolved"
    ESCALATED = "escalated"
    RESOLVED = "resolved"


class AlertType(str, PyEnum):
    CPU_HIGH = "cpu_high"
    MEMORY_HIGH = "memory_high"
    DISK_HIGH = "disk_high"
    NETWORK_ERRORS = "network_errors"
    PROCESS_CRASH = "process_crash"
    SERVICE_DOWN = "service_down"
    ANOMALY = "anomaly"


class AlertModel(Base):
    __tablename__ = "alerts"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    endpoint_id = Column(String, nullable=False, index=True)
    alert_type = Column(String, nullable=False)
    severity = Column(String, nullable=False, default="medium")
    status = Column(String, default="open", index=True)

    title = Column(String, nullable=False)
    description = Column(Text)
    metric_snapshot = Column(JSON)      # Raw metrics at time of alert

    # AI Analysis
    ai_analysis = Column(Text)           # LangChain agent analysis
    root_cause = Column(Text)
    remediation_actions = Column(JSON)   # List of actions taken
    resolution_summary = Column(Text)

    # Timing
    triggered_at = Column(DateTime, default=datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)
    acknowledged_at = Column(DateTime, nullable=True)
    acknowledged_by = Column(String, nullable=True)


class RemediationAction(BaseModel):
    action: str
    target: Optional[str] = None
    result: Optional[str] = None
    success: bool = False
    executed_at: datetime = Field(default_factory=datetime.utcnow)


class AlertCreate(BaseModel):
    endpoint_id: str
    alert_type: AlertType
    severity: AlertSeverity
    title: str
    description: str
    metric_snapshot: Optional[Dict[str, Any]] = None


class AlertResponse(BaseModel):
    id: str
    endpoint_id: str
    alert_type: str
    severity: str
    status: str
    title: str
    description: str
    ai_analysis: Optional[str]
    root_cause: Optional[str]
    remediation_actions: Optional[List[Dict]]
    resolution_summary: Optional[str]
    triggered_at: datetime
    resolved_at: Optional[datetime]

    class Config:
        from_attributes = True


class AlertAnalysisRequest(BaseModel):
    endpoint_id: str
    alert_id: Optional[str] = None
    context: Optional[str] = None
    force_reanalyze: bool = False