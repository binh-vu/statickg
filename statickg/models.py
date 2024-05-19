from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class InputFile:
    key: str
    relpath: str
    path: Path


@dataclass
class FileProcessStatus:
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
class PipelineConfig:
    """Configuration to run a pipeline"""

    convert_files: list[str]
