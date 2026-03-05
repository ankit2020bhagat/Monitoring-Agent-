"""
WebSocket endpoint for real-time telemetry streaming.
Supports 100+ concurrent connections per endpoint using Redis Streams as the backbone.
"""
import asyncio
import json
from datetime import datetime
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from typing import Optional, Dict, Set
import structlog

from app.core.redis_client import get_redis, RedisCache

router = APIRouter(tags=["websocket"])
logger = structlog.get_logger()


class ConnectionManager:
    """Manages concurrent WebSocket connections with efficient pub/sub."""

    def __init__(self):
        # endpoint_id -> set of WebSocket connections
        self._connections: Dict[str, Set[WebSocket]] = {}
        self._total_connections = 0

    async def connect(self, ws: WebSocket, endpoint_id: str):
        await ws.accept()
        if endpoint_id not in self._connections:
            self._connections[endpoint_id] = set()
        self._connections[endpoint_id].add(ws)
        self._total_connections += 1
        logger.info("ws_connected", endpoint_id=endpoint_id, total=self._total_connections)

    def disconnect(self, ws: WebSocket, endpoint_id: str):
        if endpoint_id in self._connections:
            self._connections[endpoint_id].discard(ws)
            if not self._connections[endpoint_id]:
                del self._connections[endpoint_id]
        self._total_connections = max(0, self._total_connections - 1)
        logger.info("ws_disconnected", endpoint_id=endpoint_id, total=self._total_connections)

    async def broadcast(self, endpoint_id: str, message: dict):
        if endpoint_id not in self._connections:
            return
        dead = set()
        for ws in self._connections[endpoint_id]:
            try:
                await ws.send_json(message)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self.disconnect(ws, endpoint_id)

    @property
    def stats(self):
        return {
            "total_connections": self._total_connections,
            "endpoints_monitored": len(self._connections),
        }


manager = ConnectionManager()


@router.websocket("/ws/stream/{endpoint_id}")
async def stream_metrics(
        websocket: WebSocket,
        endpoint_id: str,
        last_id: str = "0",
):
    """
    WebSocket endpoint for real-time metric streaming.

    Connect to receive live telemetry for an endpoint.
    Messages are pushed as soon as new metrics arrive (via Redis Streams).

    Query params:
    - last_id: Redis Stream ID to resume from (use "0" to start from beginning, "$" for only new)
    """
    await manager.connect(websocket, endpoint_id)
    redis = await get_redis()
    cache = RedisCache(redis)
    current_id = last_id

    # Send connection acknowledgment
    await websocket.send_json({
        "type": "connected",
        "endpoint_id": endpoint_id,
        "timestamp": datetime.utcnow().isoformat(),
        "message": f"Streaming metrics for endpoint {endpoint_id}",
    })

    try:
        while True:
            try:
                # Read from Redis stream (blocks up to 1s)
                entries = await cache.read_metric_stream(
                    endpoint_id, last_id=current_id, count=10
                )
                if entries:
                    for stream_key, messages in entries:
                        for msg_id, fields in messages:
                            current_id = msg_id
                            await websocket.send_json({
                                "type": "metric",
                                "stream_id": msg_id,
                                "endpoint_id": endpoint_id,
                                "data": fields,
                                "timestamp": datetime.utcnow().isoformat(),
                            })
                else:
                    # Send heartbeat if no new data
                    await websocket.send_json({
                        "type": "heartbeat",
                        "timestamp": datetime.utcnow().isoformat(),
                    })
                    await asyncio.sleep(0.5)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("stream_error", error=str(e))
                await asyncio.sleep(1)

    except WebSocketDisconnect:
        logger.info("ws_client_disconnected", endpoint_id=endpoint_id)
    finally:
        manager.disconnect(websocket, endpoint_id)


@router.websocket("/ws/alerts")
async def stream_alerts(websocket: WebSocket):
    """
    WebSocket endpoint for real-time alert notifications (all endpoints).
    Subscribe to receive alerts as they're triggered across the system.
    """
    await websocket.accept()
    redis = await get_redis()
    pubsub = redis.pubsub()
    await pubsub.subscribe("alerts")

    logger.info("alert_ws_connected")
    await websocket.send_json({"type": "connected", "channel": "alerts"})

    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message.get("data"):
                try:
                    alert_data = json.loads(message["data"])
                    await websocket.send_json({
                        "type": "alert",
                        "data": alert_data,
                        "timestamp": datetime.utcnow().isoformat(),
                    })
                except json.JSONDecodeError:
                    pass
            else:
                await websocket.send_json({"type": "heartbeat"})
                await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        logger.info("alert_ws_disconnected")
    finally:
        await pubsub.unsubscribe("alerts")


@router.get("/ws/stats", tags=["websocket"])
async def ws_stats():
    """Get current WebSocket connection statistics."""
    return manager.stats