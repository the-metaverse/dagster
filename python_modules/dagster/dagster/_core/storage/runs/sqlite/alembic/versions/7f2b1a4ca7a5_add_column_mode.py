"""add column "mode"

Revision ID: 7f2b1a4ca7a5
Revises: 72686963a802
Create Date: 2021-07-06 13:49:53.668345

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine import reflection

# revision identifiers, used by Alembic.
revision = "7f2b1a4ca7a5"
down_revision = "72686963a802"
branch_labels = None
depends_on = None

# pylint: disable=no-member


def upgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()

    if "runs" in has_tables:
        columns = [x.get("name") for x in inspector.get_columns("runs")]
        with op.batch_alter_table("runs") as batch_op:
            if "mode" not in columns:
                batch_op.add_column(sa.Column("mode", sa.Text))


def downgrade():
    bind = op.get_context().bind
    inspector = reflection.Inspector.from_engine(bind)
    has_tables = inspector.get_table_names()
    if "runs" in has_tables:
        columns = [x.get("name") for x in inspector.get_columns("runs")]

        with op.batch_alter_table("runs") as batch_op:
            if "mode" in columns:
                batch_op.drop_column("mode")
