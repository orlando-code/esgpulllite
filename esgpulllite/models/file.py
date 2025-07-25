from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from typing_extensions import NotRequired, TypedDict

from esgpulllite.models.base import Base
from esgpulllite.models.utils import find_str


class FileStatus(Enum):
    New = "new"
    Queued = "queued"
    Starting = "starting"
    Started = "started"
    Pausing = "pausing"
    Paused = "paused"
    Error = "error"
    Cancelled = "cancelled"
    Done = "done"

    @classmethod
    def retryable(cls) -> list[FileStatus]:
        return [cls.Error, cls.Cancelled]

    @classmethod
    def contains(cls, s: str) -> bool:
        return s in [v.value for v in cls]


class FileDict(TypedDict):
    file_id: str
    dataset_id: str
    master_id: str
    url: str
    version: str
    filename: str
    local_path: str
    data_node: str
    checksum: str
    checksum_type: str
    size: int
    status: NotRequired[str]
    variable: str
    mip_era: str
    institution_id: str
    source_id: str
    experiment_id: str
    member_id: str
    table_id: str
    grid: str
    grid_label: str
    nominal_resolution: str
    creation_date: str
    title: str
    instance_id: str
    datetime_start: str
    datetime_end: str
    citation_url: str


@dataclass(init=False)
class FastFile:
    sha: str
    file_id: str
    checksum: str

    def _as_bytes(self) -> bytes:
        self_tuple = (self.file_id, self.checksum)
        return str(self_tuple).encode()

    @classmethod
    def serialize(cls, source: dict) -> FastFile:
        result = cls()
        dataset_id = find_str(source["dataset_id"]).partition("|")[0]
        filename = find_str(source["title"])
        result.file_id = ".".join([dataset_id, filename])
        result.checksum = find_str(source["checksum"])
        Base.compute_sha(result)  # type: ignore
        return result
