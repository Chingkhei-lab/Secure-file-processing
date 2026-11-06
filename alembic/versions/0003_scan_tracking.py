"""scan tracking: attempts, scan_signature, scanned_at, cleanup index

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-04
"""
from alembic import op
import sqlalchemy as sa

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "upload_jobs",
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column("upload_jobs", sa.Column("scan_signature", sa.String(200), nullable=True))
    op.add_column(
        "upload_jobs", sa.Column("scanned_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.create_index(
        "ix_upload_jobs_infected_pending",
        "upload_jobs",
        ["status"],
        postgresql_where=sa.text("status = 'infected' AND quarantine_key IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_upload_jobs_infected_pending", table_name="upload_jobs")
    op.drop_column("upload_jobs", "scanned_at")
    op.drop_column("upload_jobs", "scan_signature")
    op.drop_column("upload_jobs", "attempts")
