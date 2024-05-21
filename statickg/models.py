from __future__ import annotations

import hashlib
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from fnmatch import fnmatch
from pathlib import Path
from typing import MutableMapping, TypeAlias

import serde.yaml
from drepr.main import convert
from loguru import logger

from kgbuilder.kgbuilder.helper import import_func

Pattern: TypeAlias = str


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


class Repository(ABC):
    @abstractmethod
    def glob(self, relpath: Pattern) -> list[InputFile]:
        raise NotImplementedError()


class GitRepository(Repository):
    def __init__(self, repo: Path):
        self.repo = repo
        self.branch2files = {}

    def glob(self, relpath: Pattern) -> list[InputFile]:
        matched_files = []
        for file in self.all_files():
            if fnmatch(file.relpath, relpath):
                matched_files.append(file)
        return matched_files

    def all_files(self, branch: str = "main") -> list[InputFile]:
        if branch not in self.branch2files:
            output = subprocess.check_output(
                ["git", "ls-tree", "-r", branch], cwd=self.repo
            )

            content = output.decode().strip().split("\n")
            files = []
            for line in content:
                objectmode, objecttype, objectname, relpath = line.split()
                assert objecttype == "blob"
                files.append(
                    InputFile(relpath=relpath, path=self.repo / relpath, key=objectname)
                )
            self.branch2files[branch] = files

        return self.branch2files[branch]


class ExtractorType(str, Enum):
    DRepr = "drepr"


@dataclass
class Extractor:
    name: str
    type: ExtractorType
    args: dict
    ext: str

    def to_dict(self):
        return {
            "name": self.name,
            "type": self.type.value,
            "args": self.args,
            "ext": self.ext,
        }

    def create(
        self, cache_dir: Path, cache_db: MutableMapping[str, ProcessStatus]
    ) -> ExtractorImpl:
        if self.type == ExtractorType.DRepr:
            return DReprExtractor(cache_dir, cache_db, self.args)
        raise NotImplementedError(self.type)


class ExtractorImpl(ABC):
    @abstractmethod
    def extract(self, infile: Path, outfile: Path):
        raise NotImplementedError()


class DReprExtractor(ExtractorImpl):

    def __init__(
        self, cache_dir: Path, cache_db: MutableMapping[str, ProcessStatus], args: dict
    ):
        file = Path(args["path"])
        key = hashlib.sha256(file.read_bytes()).hexdigest()

        outfile = cache_dir / f"{file.stem}.py"
        rebuild = True

        if outfile.exists() and args["path"] in cache_db:
            status = cache_db[args["path"]]
            if status.key == key and status.is_success:
                rebuild = False

        if rebuild:
            convert(repr=file, resources={}, progfile=outfile)
            cache_db[args["path"]] = ProcessStatus(key=key, is_success=True)
            logger.info("[drepr] generate {}", args["path"])
        else:
            logger.info("[drepr] skip {}", args["path"])

        self.format = args["format"]
        self.program = import_func(f"{outfile.parent}.{outfile.stem}.main")

    def extract(self, infile: Path, outfile: Path):
        assert self.format == "ttl"
        output = self.program(infile)
        outfile.write_text(output)


@dataclass
class ExtractInput:
    name: str
    path: Pattern
    extractor: str

    def to_dict(self):
        return {
            "name": self.name,
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

        pipeline = ETLConfig(extractors={}, inputs={})
        for extractor in cfg["extractors"]:
            assert (
                extractor["name"] not in pipeline.extractors
            ), f"Extractor {extractor['name']} is duplicated"
            pipeline.extractors[extractor["name"]] = Extractor(
                name=extractor["name"],
                type=ExtractorType(extractor["type"]),
                args=extractor["args"],
                ext=extractor["ext"],
            )

        for name, input in cfg["inputs"].items():
            assert name not in pipeline.inputs, f"Input {name} is duplicated"
            pipeline.inputs[name] = ExtractInput(
                name=name,
                path=input["path"],
                extractor=input["extractor"],
            )

        return pipeline

    def to_dict(self):
        return {
            "extractors": [
                extractor.to_dict() for extractor in self.extractors.values()
            ],
            "inputs": [input.to_dict() for input in self.inputs.values()],
        }
