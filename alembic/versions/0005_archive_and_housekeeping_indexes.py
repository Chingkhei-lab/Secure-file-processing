"""archive table and housekeeping indexes

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-05

* upload_jobs_archive: cold storage for finished job rows (see app/archive.py).
  Same columns as upload_jobs, no foreign keys (it is a record, not live state)
  and only the index its lookups need.
* Two partial indexes on upload_jobs for the housekeeping queries. Both are
  tiny compared with the table because they only cover finished jobs.
"""
from alembic import op
import sqlalchemy as sa

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE TABLE upload_jobs_archive (LIKE upload_jobs INCLUDING DEFAULTS)")
    op.execute("ALTER TABLE upload_jobs_archive ADD PRIMARY KEY (id)")
    op.create_index(
        "ix_upload_jobs_archive_owner_created",
        "upload_jobs_archive",
        ["api_key_id", "created_at"],
    )

    op.create_index(
        "ix_upload_jobs_failed_retained",
        "upload_jobs",
        ["completed_at"],
        postgresql_where=sa.text("status = 'failed' AND quarantine_key IS NOT NULL"),
    )
    op.create_index(
        "ix_upload_jobs_terminal_completed",
        "upload_jobs",
        ["completed_at"],
        postgresql_where=sa.text("status IN ('stored', 'infected', 'failed')"),
    )


def downgrade() -> None:
    op.drop_index("ix_upload_jobs_terminal_completed", table_name="upload_jobs")
    op.drop_index("ix_upload_jobs_failed_retained", table_name="upload_jobs")
    # Put archived rows back so nothing is lost (skipping any whose API key
    # has since been deleted, which the foreign key would refuse).
    op.execute(
        """
        INSERT INTO upload_jobs
        SELECT a.* FROM upload_jobs_archive a
        WHERE EXISTS (SELECT 1 FROM api_keys k WHERE k.id = a.api_key_id)
        ON CONFLICT (id) DO NOTHING
        """
    )
    op.drop_index("ix_upload_jobs_archive_owner_created", table_name="upload_jobs_archive")
    op.drop_table("upload_jobs_archive")
