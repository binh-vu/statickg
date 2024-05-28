from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TypeAlias


@dataclass
class InputFile:
    key: str
    relpath: str
    path: Path


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


class BaseType(str, Enum):
    CFG_DIR = "CFG_DIR"
    REPO = "REPO"
    DATA_DIR = "DATA_DIR"


@dataclass
class RelPath:
    basetype: BaseType
    basepath: Path
    relpath: str

    def get_path(self):
        return self.basepath / self.relpath

    def get_str(self):
        return f"::{self.basetype.value}::{self.relpath}"
