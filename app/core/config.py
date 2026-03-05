from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
import os


class Settings(BaseSettings):
    # App
    app_name: str = "MonitoringAgent"
    app_env: str = "development"
    debug: bool = False
    secret_key: str = "change-me-in-production"

    # Database
    database_url: str = "postgresql+asyncpg://postgres:password@localhost:5432/monitoring_db"

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_cache_ttl: int = 300

    # AI
    anthropic_api_key: Optional[str] = None
    openai_api_key: Optional[str] = None
    ai_provider: str = "anthropic"
    ai_model: str = "claude-sonnet-4-20250514"

    # Thresholds
    cpu_alert_threshold: float = 85.0
    memory_alert_threshold: float = 90.0
    disk_alert_threshold: float = 85.0
    network_error_threshold: int = 100
    collection_interval: int = 15

    # Alerts
    alert_cooldown_seconds: int = 300
    auto_remediation_enabled: bool = True
    escalation_email: str = "admin@example.com"

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()