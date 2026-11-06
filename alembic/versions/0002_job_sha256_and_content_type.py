"""add sha256 and validated content_type to upload_jobs

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-04
"""
from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("upload_jobs", sa.Column("content_type", sa.String(100), nullable=True))
    op.add_column("upload_jobs", sa.Column("sha256", sa.String(64), nullable=True))


def downgrade() -> None:
    op.drop_column("upload_jobs", "sha256")
    op.drop_column("upload_jobs", "content_type")
