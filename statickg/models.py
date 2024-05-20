from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TypeAlias

import serde.yaml

Pattern: TypeAlias = str


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


class Repository:
    def glob(self, path: Pattern) -> list[InputFile]:
        raise NotImplementedError()


class ExtractorType(str, Enum):
    DRepr = "drepr"


@dataclass
class Extractor:
    name: str
    type: ExtractorType
    args: dict

    def to_dict(self):
        return {
            "name": self.name,
            "type": self.type.value,
            "args": self.args,
        }


@dataclass
class ExtractInput:
    path: Pattern
    extractor: str

    def to_dict(self):
        return {
            "path": self.path,
            "extractor": self.extractor,
        }


@dataclass
class ETLConfig:
    """Configuration to run a pipeline"""

    extractors: dict[str, Extractor]
    inputs: dict[str, ExtractInput]

    @staticmethod
    def parse(infile: Path):
        cfg = serde.yaml.deser(infile)
        assert cfg.version == 1

        pipeline = ETLConfig(extractors={}, inputs=[])
        for extractor in cfg["extractors"]:
            assert (
                extractor["name"] not in pipeline.extractors
            ), f"Extractor {extractor['name']} is duplicated"
            pipeline.extractors[extractor["name"]] = Extractor(
                name=extractor["name"],
                type=ExtractorType(extractor["type"]),
                args=extractor["args"],
            )

        for name, input in cfg["inputs"].items():
            assert name not in pipeline.inputs, f"Input {name} is duplicated"
            pipeline.inputs[name] = ExtractInput(
                path=input["path"],
                extractor=input["extractor"],
            )

        return pipeline

    def to_dict(self):
        return {
            "extractors": [
                extractor.to_dict() for extractor in self.extractors.values()
            ],
            "inputs": [input.to_dict() for input in self.inputs],
        }
