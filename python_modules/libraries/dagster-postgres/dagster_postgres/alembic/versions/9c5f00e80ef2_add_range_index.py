"""add range index

Revision ID: 9c5f00e80ef2
Revises: 42add02bf976
Create Date: 2022-01-20 11:43:21.070463

"""
from dagster._core.storage.migration.utils import create_run_range_indices

# revision identifiers, used by Alembic.
revision = "9c5f00e80ef2"
down_revision = "42add02bf976"
branch_labels = None
depends_on = None


def upgrade():
    create_run_range_indices()


def downgrade():
    pass
