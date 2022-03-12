"""change varchar to text

Revision ID: 0da417ae1b81
Revises: 942138e33bf9
Create Date: 2021-01-14 12:29:33.410870

"""
import sqlalchemy as sa
from alembic import op

from dagster._core.storage.migration.utils import has_table

# pylint: disable=no-member

# revision identifiers, used by Alembic.
revision = "0da417ae1b81"
down_revision = "942138e33bf9"
branch_labels = None
depends_on = None


def upgrade():
    if has_table("event_logs"):
        with op.batch_alter_table("event_logs") as batch_op:
            batch_op.alter_column(
                "step_key",
                type_=sa.Text,
                existing_type=sa.String,
            )
            batch_op.alter_column(
                "asset_key",
                type_=sa.Text,
                existing_type=sa.String,
            )
            batch_op.alter_column(
                "partition",
                type_=sa.Text,
                existing_type=sa.String,
            )

    if has_table("secondary_indexes"):
        with op.batch_alter_table("secondary_indexes") as batch_op:
            batch_op.alter_column(
                "name",
                type_=sa.Text,
                existing_type=sa.String,
            )

    if has_table("asset_keys"):
        with op.batch_alter_table("asset_keys") as batch_op:
            batch_op.alter_column(
                "asset_key",
                type_=sa.Text,
                existing_type=sa.String,
            )


def downgrade():
    if has_table("event_logs"):
        with op.batch_alter_table("event_logs") as batch_op:
            batch_op.alter_column(
                "step_key",
                type_=sa.String,
                existing_type=sa.Text,
            )
            batch_op.alter_column(
                "asset_key",
                type_=sa.String,
                existing_type=sa.Text,
            )
            batch_op.alter_column(
                "partition",
                type_=sa.String,
                existing_type=sa.Text,
            )

    if has_table("secondary_indexes"):
        with op.batch_alter_table("secondary_indexes") as batch_op:
            batch_op.alter_column(
                "name",
                type_=sa.String,
                existing_type=sa.Text,
            )

    if has_table("asset_keys"):
        with op.batch_alter_table("asset_keys") as batch_op:
            batch_op.alter_column(
                "asset_key",
                type_=sa.String,
                existing_type=sa.Text,
            )
