from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.orm import Mapped, mapped_column

from esgpulllite.models.base import Base


class Tag(Base):
    __tablename__ = "tag"

    name: Mapped[str] = mapped_column(sa.String(255))
    description: Mapped[str | None] = mapped_column(sa.Text, default=None)

    def _as_bytes(self) -> bytes:
        return self.name.encode()

    def __hash__(self) -> int:
        return hash(self._as_bytes())


@event.listens_for(Tag, "before_insert")
def ensure_sha_before_insert(mapper, connection, target: Tag) -> None:
    """
    Ensure the 'sha' attribute is computed if it's not already set
    before a Tag instance is inserted into the database.
    """
    if target.sha is None:
        target.compute_sha()
