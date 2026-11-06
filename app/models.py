import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class JobStatus(str, enum.Enum):
    QUARANTINED = "quarantined"  # received, waiting in quarantine
    SCANNING = "scanning"        # worker is running ClamAV
    CLEAN = "clean"              # scan passed, not yet stored
    STORING = "storing"          # worker is compressing / writing final copy
    STORED = "stored"            # compressed and in final S3 location
    INFECTED = "infected"        # malware found, file discarded
    FAILED = "failed"            # processing error


class Compression(str, enum.Enum):
    NONE = "none"
    ZSTD = "zstd"
    WEBP = "webp"
    PDF_OPT = "pdf_opt"


def _values(enum_cls: type[enum.Enum]) -> str:
    return ", ".join(f"'{m.value}'" for m in enum_cls)


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100))
    # We store only a SHA-256 hash of the key, never the key itself.
    key_hash: Mapped[str] = mapped_column(String(64), unique=True)
    # Short non-secret prefix so humans can tell keys apart in logs/admin.
    key_prefix: Mapped[str] = mapped_column(String(12))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    jobs: Mapped[list["UploadJob"]] = relationship(back_populates="api_key")


class StoredObject(Base):
    """One row per unique blob in S3. Identical files share a single row."""

    __tablename__ = "stored_objects"
    __table_args__ = (
        CheckConstraint(
            f"compression IN ({_values(Compression)})", name="ck_stored_objects_compression"
        ),
        CheckConstraint("original_size >= 0 AND stored_size >= 0", name="ck_stored_objects_sizes"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    # SHA-256 of the ORIGINAL bytes, hex. Unique => this is the dedupe key.
    sha256: Mapped[str] = mapped_column(String(64), unique=True)
    content_type: Mapped[str] = mapped_column(String(100))
    original_size: Mapped[int] = mapped_column(BigInteger)
    stored_size: Mapped[int] = mapped_column(BigInteger)
    compression: Mapped[str] = mapped_column(String(16), default=Compression.NONE.value)
    s3_key: Mapped[str] = mapped_column(String(255), unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    jobs: Mapped[list["UploadJob"]] = relationship(back_populates="stored_object")


class UploadJob(Base):
    """One row per upload request. Many jobs may point at one StoredObject."""

    __tablename__ = "upload_jobs"
    __table_args__ = (
        CheckConstraint(f"status IN ({_values(JobStatus)})", name="ck_upload_jobs_status"),
        CheckConstraint("size_bytes >= 0", name="ck_upload_jobs_size"),
        # "Show me my recent uploads" - the most common query.
        Index("ix_upload_jobs_owner_created", "api_key_id", "created_at"),
        # Partial index: workers only look at unfinished jobs, which are a
        # tiny fraction of the table. Keeps the index small and fast.
        Index(
            "ix_upload_jobs_in_flight",
            "status",
            postgresql_where="status IN ('quarantined', 'scanning', 'clean', 'storing')",
        ),
        Index("ix_upload_jobs_stored_object", "stored_object_id"),
        # Failed jobs still holding a quarantined file: the retention janitor
        # looks for the ones past their retention period.
        Index(
            "ix_upload_jobs_failed_retained",
            "completed_at",
            postgresql_where="status = 'failed' AND quarantine_key IS NOT NULL",
        ),
        # Finished jobs by completion time: lets the archive routine find the
        # oldest ones without scanning the whole table.
        Index(
            "ix_upload_jobs_terminal_completed",
            "completed_at",
            postgresql_where="status IN ('stored', 'infected', 'failed')",
        ),
        # Finished jobs whose quarantined copy still needs deleting (rare).
        Index(
            "ix_upload_jobs_cleanup_pending",
            "status",
            postgresql_where="status IN ('infected', 'stored') AND quarantine_key IS NOT NULL",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    api_key_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("api_keys.id"))
    original_filename: Mapped[str] = mapped_column(String(255))
    declared_content_type: Mapped[str | None] = mapped_column(String(100))
    # What the file really is, decided by us from its bytes (not the client).
    content_type: Mapped[str | None] = mapped_column(String(100))
    # SHA-256 of the uploaded bytes, computed while streaming.
    sha256: Mapped[str | None] = mapped_column(String(64))
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(16), default=JobStatus.QUARANTINED.value)
    error: Mapped[str | None] = mapped_column(Text)
    # How many times a worker has started scanning this job.
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    # How many times a worker has started the compress-and-store step.
    store_attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    # True if this upload reused an object that already existed. Internal
    # only: never returned by the API (see README: dedupe side channel).
    deduplicated: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    # Name of the malware signature if the scan found something.
    scan_signature: Mapped[str | None] = mapped_column(String(200))
    scanned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Where the raw file sits while waiting to be scanned (cleared afterwards).
    quarantine_key: Mapped[str | None] = mapped_column(String(255))
    stored_object_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("stored_objects.id")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    api_key: Mapped[ApiKey] = relationship(back_populates="jobs")
    stored_object: Mapped[StoredObject | None] = relationship(back_populates="jobs")
