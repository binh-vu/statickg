from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TypeAlias


class BaseType(str, Enum):
    CFG_DIR = "CFG_DIR"
    REPO = "REPO"
    DATA_DIR = "DATA_DIR"
    WORK_DIR = "WORK_DIR"
    DB_DIR = "DB_DIR"


@dataclass
class InputFile:
    basetype: BaseType
    key: str
    relpath: str
    path: Path

    def get_path_ident(self):
        return get_ident(self.basetype, self.relpath)


@dataclass
class ProcessStatus:
    key: str
    is_success: bool

    def to_dict(self):
        return {
            "key": self.key,
            "is_success": self.is_success,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            key=data["key"],
            is_success=data["is_success"],
        )


@dataclass
class RelPath:
    basetype: BaseType
    basepath: Path
    relpath: str

    def get_path(self):
        return self.basepath / self.relpath

    def get_ident(self):
        return get_ident(self.basetype, self.relpath)


def get_ident(base: BaseType, relpath: str) -> str:
    return f"::{base.value}::{relpath}"
