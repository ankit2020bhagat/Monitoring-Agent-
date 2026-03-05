from sqlalchemy import Column, String, Float, Integer, DateTime, JSON, Text, Index
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import uuid

from app.core.database import Base


# ─── SQLAlchemy ORM Models ────────────────────────────────────────────────────

class EndpointModel(Base):
    __tablename__ = "endpoints"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    hostname = Column(String, nullable=False, index=True)
    ip_address = Column(String, nullable=False)
    os_info = Column(JSON, nullable=True)
    tags = Column(JSON, default=list)
    registered_at = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Integer, default=1)


class MetricModel(Base):
    __tablename__ = "metrics"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    endpoint_id = Column(String, nullable=False, index=True)
    collected_at = Column(DateTime, default=datetime.utcnow, index=True)

    # CPU
    cpu_percent = Column(Float)
    cpu_count = Column(Integer)
    cpu_freq_mhz = Column(Float)
    load_avg_1m = Column(Float)
    load_avg_5m = Column(Float)
    load_avg_15m = Column(Float)

    # Memory
    memory_total_gb = Column(Float)
    memory_used_gb = Column(Float)
    memory_percent = Column(Float)
    swap_percent = Column(Float)

    # Disk
    disk_total_gb = Column(Float)
    disk_used_gb = Column(Float)
    disk_percent = Column(Float)
    disk_read_mb = Column(Float)
    disk_write_mb = Column(Float)

    # Network
    net_bytes_sent_mb = Column(Float)
    net_bytes_recv_mb = Column(Float)
    net_packets_sent = Column(Integer)
    net_packets_recv = Column(Integer)
    net_errors_in = Column(Integer)
    net_errors_out = Column(Integer)

    # Process count
    process_count = Column(Integer)
    top_processes = Column(JSON)

    __table_args__ = (
        Index("ix_metrics_endpoint_time", "endpoint_id", "collected_at"),
    )


# ─── Pydantic Schemas ─────────────────────────────────────────────────────────

class CPUMetrics(BaseModel):
    percent: float = Field(..., ge=0, le=100)
    count: int
    freq_mhz: Optional[float] = None
    load_avg_1m: float = 0.0
    load_avg_5m: float = 0.0
    load_avg_15m: float = 0.0


class MemoryMetrics(BaseModel):
    total_gb: float
    used_gb: float
    percent: float = Field(..., ge=0, le=100)
    swap_percent: float = 0.0


class DiskMetrics(BaseModel):
    total_gb: float
    used_gb: float
    percent: float = Field(..., ge=0, le=100)
    read_mb: float = 0.0
    write_mb: float = 0.0


class NetworkMetrics(BaseModel):
    bytes_sent_mb: float = 0.0
    bytes_recv_mb: float = 0.0
    packets_sent: int = 0
    packets_recv: int = 0
    errors_in: int = 0
    errors_out: int = 0


class ProcessInfo(BaseModel):
    pid: int
    name: str
    cpu_percent: float
    memory_mb: float
    status: str


class TelemetryPayload(BaseModel):
    endpoint_id: str
    hostname: str
    ip_address: str
    collected_at: datetime = Field(default_factory=datetime.utcnow)
    cpu: CPUMetrics
    memory: MemoryMetrics
    disk: DiskMetrics
    network: NetworkMetrics
    process_count: int = 0
    top_processes: List[ProcessInfo] = []
    os_info: Optional[Dict[str, Any]] = None
    tags: List[str] = []


class TelemetryBatch(BaseModel):
    """Batch ingestion of multiple metric snapshots."""
    metrics: List[TelemetryPayload] = Field(..., min_length=1, max_length=500)


class EndpointCreate(BaseModel):
    hostname: str
    ip_address: str
    os_info: Optional[Dict[str, Any]] = None
    tags: List[str] = []


class EndpointResponse(BaseModel):
    id: str
    hostname: str
    ip_address: str
    os_info: Optional[Dict[str, Any]]
    tags: List[str]
    registered_at: datetime
    last_seen: datetime
    is_active: bool

    class Config:
        from_attributes = True


class MetricSummary(BaseModel):
    endpoint_id: str
    period_start: datetime
    period_end: datetime
    avg_cpu_percent: float
    max_cpu_percent: float
    avg_memory_percent: float
    max_memory_percent: float
    avg_disk_percent: float
    data_points: int