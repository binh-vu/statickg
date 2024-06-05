from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TypeAlias, Union


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
        if self.relpath != "":
            return self.basepath / self.relpath
        return self.basepath

    def get_ident(self):
        return get_ident(self.basetype, self.relpath)


@dataclass
class RefPathRef:
    start: int
    end: int  # exclusive
    relpath: RelPath


@dataclass
class RelPathRefStr:
    """String that contains references to relative paths"""

    # list of references (start, end, relpath) in the string, the end is exclusive
    # the list must not empty
    refs: list[RefPathRef]
    # the string itself
    value: str

    def deref(self) -> str:
        chunks = [self.value[0 : self.refs[0].start]]
        chunks.extend(
            (
                str(ref.relpath.get_path())
                + self.value[
                    ref.end : (
                        self.refs[i + 1].start if i + 1 < len(self.refs) else None
                    )
                ]
                for i, ref in enumerate(self.refs)
            )
        )
        return "".join(chunks)


RelPathRefStrOrStr = Union[RelPathRefStr, str]


def get_ident(base: BaseType, relpath: str) -> str:
    return f"::{base.value}::{relpath}"
