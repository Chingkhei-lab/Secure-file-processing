"""store step: 'storing' status, store_attempts, deduplicated, cleanup index

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-05
"""
from alembic import op
import sqlalchemy as sa

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None

OLD_STATUSES = "'quarantined', 'scanning', 'clean', 'stored', 'infected', 'failed'"
NEW_STATUSES = "'quarantined', 'scanning', 'clean', 'storing', 'stored', 'infected', 'failed'"


def upgrade() -> None:
    op.add_column(
        "upload_jobs",
        sa.Column("store_attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "upload_jobs",
        sa.Column("deduplicated", sa.Boolean(), nullable=False, server_default="false"),
    )

    op.drop_constraint("ck_upload_jobs_status", "upload_jobs", type_="check")
    op.create_check_constraint(
        "ck_upload_jobs_status", "upload_jobs", f"status IN ({NEW_STATUSES})"
    )

    op.drop_index("ix_upload_jobs_in_flight", table_name="upload_jobs")
    op.create_index(
        "ix_upload_jobs_in_flight",
        "upload_jobs",
        ["status"],
        postgresql_where=sa.text("status IN ('quarantined', 'scanning', 'clean', 'storing')"),
    )

    op.drop_index("ix_upload_jobs_infected_pending", table_name="upload_jobs")
    op.create_index(
        "ix_upload_jobs_cleanup_pending",
        "upload_jobs",
        ["status"],
        postgresql_where=sa.text(
            "status IN ('infected', 'stored') AND quarantine_key IS NOT NULL"
        ),
    )


def downgrade() -> None:
    op.drop_index("ix_upload_jobs_cleanup_pending", table_name="upload_jobs")
    op.create_index(
        "ix_upload_jobs_infected_pending",
        "upload_jobs",
        ["status"],
        postgresql_where=sa.text("status = 'infected' AND quarantine_key IS NOT NULL"),
    )

    # A job mid-store goes back to waiting; the old code has no 'storing'.
    op.execute("UPDATE upload_jobs SET status = 'clean' WHERE status = 'storing'")
    op.drop_index("ix_upload_jobs_in_flight", table_name="upload_jobs")
    op.create_index(
        "ix_upload_jobs_in_flight",
        "upload_jobs",
        ["status"],
        postgresql_where=sa.text("status IN ('quarantined', 'scanning', 'clean')"),
    )
    op.drop_constraint("ck_upload_jobs_status", "upload_jobs", type_="check")
    op.create_check_constraint(
        "ck_upload_jobs_status", "upload_jobs", f"status IN ({OLD_STATUSES})"
    )

    op.drop_column("upload_jobs", "deduplicated")
    op.drop_column("upload_jobs", "store_attempts")
