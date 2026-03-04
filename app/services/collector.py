
import asyncio
import psutil
import platform
import uuid
import socket
from datetime import datetime
from typing import Optional, List
import httpx
import structlog

from app.models.telemetry import (
    TelemetryPayload, CPUMetrics, MemoryMetrics, DiskMetrics,
    NetworkMetrics, ProcessInfo
)
from app.core.config import settings

logger = structlog.get_logger()


def get_endpoint_id() -> str:
    """Stable endpoint ID based on hostname + MAC address."""
    mac = hex(uuid.getnode())[2:]
    hostname = socket.gethostname()
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{hostname}-{mac}"))


def collect_metrics(endpoint_id: Optional[str] = None) -> TelemetryPayload:
    """Synchronously collect all system metrics."""
    endpoint_id = endpoint_id or get_endpoint_id()

    # CPU
    cpu_freq = psutil.cpu_freq()
    load = psutil.getloadavg()
    cpu = CPUMetrics(
        percent=psutil.cpu_percent(interval=0.5),
        count=psutil.cpu_count(logical=True),
        freq_mhz=cpu_freq.current if cpu_freq else None,
        load_avg_1m=load[0],
        load_avg_5m=load[1],
        load_avg_15m=load[2],
    )

    # Memory
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    memory = MemoryMetrics(
        total_gb=round(mem.total / 1e9, 3),
        used_gb=round(mem.used / 1e9, 3),
        percent=mem.percent,
        swap_percent=swap.percent,
    )

    # Disk
    disk = psutil.disk_usage("/")
    io = psutil.disk_io_counters()
    disk_metrics = DiskMetrics(
        total_gb=round(disk.total / 1e9, 3),
        used_gb=round(disk.used / 1e9, 3),
        percent=disk.percent,
        read_mb=round(io.read_bytes / 1e6, 2) if io else 0,
        write_mb=round(io.write_bytes / 1e6, 2) if io else 0,
    )

    # Network
    net = psutil.net_io_counters()
    network = NetworkMetrics(
        bytes_sent_mb=round(net.bytes_sent / 1e6, 2),
        bytes_recv_mb=round(net.bytes_recv / 1e6, 2),
        packets_sent=net.packets_sent,
        packets_recv=net.packets_recv,
        errors_in=net.errin,
        errors_out=net.errout,
    )

    # Top processes
    processes: List[ProcessInfo] = []
    for proc in sorted(
            psutil.process_iter(["pid", "name", "cpu_percent", "memory_info", "status"]),
            key=lambda p: p.info.get("cpu_percent") or 0,
            reverse=True,
    )[:10]:
        try:
            info = proc.info
            if info["memory_info"]:
                processes.append(ProcessInfo(
                    pid=info["pid"],
                    name=info["name"],
                    cpu_percent=info["cpu_percent"] or 0.0,
                    memory_mb=round(info["memory_info"].rss / 1e6, 2),
                    status=info["status"],
                ))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    os_info = {
        "system": platform.system(),
        "release": platform.release(),
        "version": platform.version(),
        "machine": platform.machine(),
        "python": platform.python_version(),
    }

    return TelemetryPayload(
        endpoint_id=endpoint_id,
        hostname=socket.gethostname(),
        ip_address=socket.gethostbyname(socket.gethostname()),
        collected_at=datetime.utcnow(),
        cpu=cpu,
        memory=memory,
        disk=disk_metrics,
        network=network,
        process_count=len(psutil.pids()),
        top_processes=processes,
        os_info=os_info,
    )


class MetricsCollector:


    def __init__(
            self,
            server_url: str,
            endpoint_id: Optional[str] = None,
            interval: int = 15,
    ):
        self.server_url = server_url.rstrip("/")
        self.endpoint_id = endpoint_id or get_endpoint_id()
        self.interval = interval
        self._running = False
        self._client: Optional[httpx.AsyncClient] = None

    async def start(self):
        self._running = True
        self._client = httpx.AsyncClient(
            base_url=self.server_url,
            timeout=10.0,
            limits=httpx.Limits(max_keepalive_connections=5),
        )
        logger.info("collector_started", endpoint_id=self.endpoint_id, interval=self.interval)
        await self._collect_loop()

    async def stop(self):
        self._running = False
        if self._client:
            await self._client.aclose()
        logger.info("collector_stopped")

    async def _collect_loop(self):
        while self._running:
            try:
                payload = await asyncio.get_event_loop().run_in_executor(
                    None, collect_metrics, self.endpoint_id
                )
                await self._send(payload)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("collection_error", error=str(e))

            await asyncio.sleep(self.interval)

    async def _send(self, payload: TelemetryPayload):
        """POST metrics to the server with retry logic."""
        for attempt in range(3):
            try:
                resp = await self._client.post(
                    "/api/v1/telemetry/ingest",
                    json=payload.model_dump(mode="json"),
                )
                resp.raise_for_status()
                logger.debug("metrics_sent", status=resp.status_code, endpoint=self.endpoint_id)
                return
            except httpx.HTTPStatusError as e:
                logger.error("send_failed", status=e.response.status_code, attempt=attempt)
            except httpx.RequestError as e:
                logger.error("send_error", error=str(e), attempt=attempt)
                await asyncio.sleep(2 ** attempt)  # exponential backoff


if __name__ == "__main__":
    import sys

    server = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    collector = MetricsCollector(server_url=server, interval=settings.collection_interval)
    asyncio.run(collector.start())