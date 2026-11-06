from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL


class Settings(BaseSettings):
    """All configuration comes from environment variables (see .env.example)."""

    model_config = SettingsConfigDict(extra="ignore")

    app_env: str = "development"
    log_level: str = "INFO"

    # PostgreSQL
    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_host: str = "postgres"
    postgres_port: int = 5432

    # Redis
    redis_url: str = "redis://redis:6379/0"

    # S3
    s3_endpoint_url: str | None = None  # None => real AWS
    s3_bucket: str = "secure-files"
    s3_region: str = "us-east-1"

    # ClamAV
    clamav_host: str = "clamav"
    clamav_port: int = 3310

    # Request authentication
    signing_master_secret: str  # required: no default on purpose
    signature_window_seconds: int = 300
    rate_limit_per_minute: int = 60

    # Uploads
    max_upload_bytes: int = 25 * 1024 * 1024  # 25 MB

    # Compression
    image_mode: str = "lossy"  # "lossy" or "lossless" WebP
    image_quality: int = 80  # lossy WebP quality, 1-100
    zstd_level: int = 15  # 1 (fast) .. 19 (smallest)
    max_image_pixels: int = 50_000_000  # reject bigger images (decompression bombs)

    # Housekeeping and retention
    failed_retention_days: int = 7  # keep a failed job's quarantined file this long
    archive_after_days: int = 90  # move finished job rows to the archive table after this

    # S3 bucket setup (python -m app.s3setup apply)
    quarantine_expire_days: int = 7  # S3 deletes leftover quarantine/ objects after this
    lifecycle_ia_days: int = 30  # move objects/ to Standard-IA after this (0 = never)
    lifecycle_glacier_days: int = 90  # then to Glacier Instant Retrieval (0 = never)

    # Used only to turn bytes into an estimated storage cost in reports
    s3_price_per_gb_month: float = 0.023  # S3 Standard, us-east-1

    @property
    def database_url(self) -> str:
        # URL.create escapes special characters in the password for us.
        return URL.create(
            "postgresql+asyncpg",
            username=self.postgres_user,
            password=self.postgres_password,
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        ).render_as_string(hide_password=False)


@lru_cache
def get_settings() -> Settings:
    return Settings()
