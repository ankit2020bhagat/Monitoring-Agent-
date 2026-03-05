import redis.asyncio as redis
from app.core.config import settings
import json
import structlog
from typing import Any, Optional

logger = structlog.get_logger()

_redis_client: Optional[redis.Redis] = None


async def get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=50,
        )
    return _redis_client


async def close_redis():
    global _redis_client
    if _redis_client:
        await _redis_client.aclose()
        _redis_client = None


class RedisCache:
    """High-level Redis cache operations for telemetry data."""

    def __init__(self, client: redis.Redis):
        self.client = client
        self.ttl = settings.redis_cache_ttl

    async def set_metrics(self, endpoint_id: str, metrics: dict, ttl: int = None):
        key = f"metrics:{endpoint_id}:latest"
        await self.client.setex(key, ttl or self.ttl, json.dumps(metrics))

    async def get_metrics(self, endpoint_id: str) -> Optional[dict]:
        key = f"metrics:{endpoint_id}:latest"
        data = await self.client.get(key)
        return json.loads(data) if data else None

    async def push_metric_stream(self, endpoint_id: str, metrics: dict, max_len: int = 1000):
        """Push to Redis Stream for real-time consumption."""
        stream_key = f"stream:{endpoint_id}"
        flat = {k: str(v) for k, v in self._flatten(metrics).items()}
        await self.client.xadd(stream_key, flat, maxlen=max_len)

    async def read_metric_stream(self, endpoint_id: str, last_id: str = "0", count: int = 100):
        stream_key = f"stream:{endpoint_id}"
        return await self.client.xread({stream_key: last_id}, count=count, block=100)

    async def set_alert_state(self, alert_key: str, data: dict, ttl: int = 86400):
        await self.client.setex(f"alert:{alert_key}", ttl, json.dumps(data))

    async def get_alert_state(self, alert_key: str) -> Optional[dict]:
        data = await self.client.get(f"alert:{alert_key}")
        return json.loads(data) if data else None

    async def get_endpoint_count(self) -> int:
        keys = await self.client.keys("metrics:*:latest")
        return len(keys)

    async def publish_alert(self, channel: str, message: dict):
        await self.client.publish(channel, json.dumps(message))

    def _flatten(self, d: dict, parent_key: str = "", sep: str = ".") -> dict:
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(self._flatten(v, new_key, sep))
            else:
                items[new_key] = v
        return items