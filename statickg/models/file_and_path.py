from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import TypeAlias, TypedDict, Union

from pydantic import BaseModel


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

    def get_ident(self):
        return self.get_path_ident() + f"::{self.key}"

    def get_path_ident(self):
        return get_ident(self.basetype, self.relpath)

    def get_basedir(self):
        basedir = self.path
        for i in range(len(Path(self.relpath).parents)):
            basedir = basedir.parent
        return basedir

    @staticmethod
    def from_relpath(relpath: RelPath):
        path = relpath.get_path()
        return InputFile(
            basetype=relpath.basetype,
            key=hashlib.sha256(path.read_bytes()).hexdigest(),
            relpath=relpath.relpath,
            path=path,
        )


class FormatOutputPath(TypedDict):
    base: RelPath
    format: str


class FormatOutputPathModel(BaseModel):
    outdir: Path
    outfile_fmt: str

    @staticmethod
    def init(obj: RelPath | FormatOutputPath):
        if isinstance(obj, RelPath):
            outdir_base = obj
            outfile_fmt = "{filestem}.{fileext}"
            outdir_path = outdir_base.get_path()
            outdir_path.mkdir(parents=True, exist_ok=True)
        else:
            outdir_base = obj["base"]
            outfile_fmt = obj["format"]
            outdir_path = outdir_base.get_path()
            outdir_path.mkdir(parents=True, exist_ok=True)

        return FormatOutputPathModel(outdir=outdir_path, outfile_fmt=outfile_fmt)

    def get_outfile(self, infile: Path):
        return self.outdir / self.outfile_fmt.format(
            fileparent=infile.parent.name,
            filegrandparent=infile.parent.parent.name,
            filestem=infile.stem,
            fileext=infile.suffix[1:],
        )


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

    @cached_property
    def suffix(self):
        return self.get_path().suffix

    @cached_property
    def stem(self):
        return self.get_path().stem

    def get_path(self):
        if self.relpath != "":
            return self.basepath / self.relpath
        return self.basepath

    def get_ident(self):
        return get_ident(self.basetype, self.relpath)

    def get_content_ident(self):
        return (
            get_ident(self.basetype, self.relpath)
            + f"::{hashlib.sha256(self.get_path().read_bytes()).hexdigest()}"
        )

    def __truediv__(self, other: str):
        return RelPath(self.basetype, self.basepath, str(Path(self.relpath) / other))

    def iterdir(self):
        return (
            RelPath(self.basetype, self.basepath, str(p.relative_to(self.basepath)))
            for p in self.get_path().iterdir()
        )

    def __str__(self):
        return self.get_ident()


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

    def __str__(self):
        return self.value


RelPathRefStrOrStr: TypeAlias = Union[RelPathRefStr, str]


def get_ident(base: BaseType, relpath: str) -> str:
    return f"::{base.value}::{relpath}"
