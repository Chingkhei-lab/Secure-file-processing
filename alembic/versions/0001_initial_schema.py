"""initial schema: api_keys, stored_objects, upload_jobs

Revision ID: 0001
Revises:
Create Date: 2026-11-04
"""
from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "api_keys",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("key_hash", sa.String(64), nullable=False),
        sa.Column("key_prefix", sa.String(12), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("key_hash", name="uq_api_keys_key_hash"),
    )

    op.create_table(
        "stored_objects",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("content_type", sa.String(100), nullable=False),
        sa.Column("original_size", sa.BigInteger(), nullable=False),
        sa.Column("stored_size", sa.BigInteger(), nullable=False),
        sa.Column("compression", sa.String(16), nullable=False),
        sa.Column("s3_key", sa.String(255), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("sha256", name="uq_stored_objects_sha256"),
        sa.UniqueConstraint("s3_key", name="uq_stored_objects_s3_key"),
        sa.CheckConstraint(
            "compression IN ('none', 'zstd', 'webp', 'pdf_opt')",
            name="ck_stored_objects_compression",
        ),
        sa.CheckConstraint(
            "original_size >= 0 AND stored_size >= 0", name="ck_stored_objects_sizes"
        ),
    )

    op.create_table(
        "upload_jobs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("api_key_id", sa.Uuid(), sa.ForeignKey("api_keys.id"), nullable=False),
        sa.Column("original_filename", sa.String(255), nullable=False),
        sa.Column("declared_content_type", sa.String(100), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("quarantine_key", sa.String(255), nullable=True),
        sa.Column(
            "stored_object_id", sa.Uuid(), sa.ForeignKey("stored_objects.id"), nullable=True
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('quarantined', 'scanning', 'clean', 'stored', 'infected', 'failed')",
            name="ck_upload_jobs_status",
        ),
        sa.CheckConstraint("size_bytes >= 0", name="ck_upload_jobs_size"),
    )

    op.create_index(
        "ix_upload_jobs_owner_created", "upload_jobs", ["api_key_id", "created_at"]
    )
    op.create_index(
        "ix_upload_jobs_in_flight",
        "upload_jobs",
        ["status"],
        postgresql_where=sa.text("status IN ('quarantined', 'scanning', 'clean')"),
    )
    op.create_index("ix_upload_jobs_stored_object", "upload_jobs", ["stored_object_id"])


def downgrade() -> None:
    op.drop_index("ix_upload_jobs_stored_object", table_name="upload_jobs")
    op.drop_index("ix_upload_jobs_in_flight", table_name="upload_jobs")
    op.drop_index("ix_upload_jobs_owner_created", table_name="upload_jobs")
    op.drop_table("upload_jobs")
    op.drop_table("stored_objects")
    op.drop_table("api_keys")
