from __future__ import annotations

import hashlib
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from fnmatch import fnmatch
from pathlib import Path
from typing import MutableMapping, Optional, TypeAlias

import serde.yaml
from drepr.main import convert
from loguru import logger

from statickg.helper import import_func

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

    @abstractmethod
    def fetch(self) -> bool:
        raise NotImplementedError()


class GitRepository(Repository):
    def __init__(self, repo: Path):
        self.repo = repo
        self.branch2files = {}
        self.current_commit = None

    def fetch(self) -> bool:
        """Fetch new data. Return True if there is new data"""
        # fetch from the remote repository
        output = subprocess.check_output(["git", "pull"], cwd=self.repo)
        if output != b"Already up to date.\n":
            self.current_commit = self.get_current_commit()
            return True

        current_commit_id = self.get_current_commit()
        if current_commit_id != self.current_commit:
            # user has manually updated the repository
            self.current_commit = current_commit_id
            return True

        return False

    def glob(self, relpath: Pattern) -> list[InputFile]:
        matched_files = {str(p.relative_to(self.repo)) for p in self.repo.glob(relpath)}
        return [file for file in self.all_files() if file.relpath in matched_files]

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

    def get_current_commit(self):
        return (
            subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.repo)
            .decode()
            .strip()
        )


class ExtractorType(str, Enum):
    DRepr = "drepr"
    Copy = "copy"


@dataclass
class Extractor:
    name: str
    type: ExtractorType
    args: dict
    ext: Optional[str]

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
        if self.type == ExtractorType.Copy:
            return CopyExtractor(self.args)
        raise NotImplementedError(self.type)


class ExtractorImpl(ABC):

    @abstractmethod
    def get_key(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def extract(self, infile: Path, outfile: Path):
        raise NotImplementedError()


class CopyExtractor(ExtractorImpl):
    def __init__(self, *args, **kwargs):
        pass

    def get_key(self):
        return "copy"

    def extract(self, infile: Path, outfile: Path):
        outfile.write_bytes(infile.read_bytes())


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
        assert self.format in {"turtle"}, self.format
        self.program = import_func(f"{outfile.parent.name}.{outfile.stem}.main")
        self.key = key

    def get_key(self):
        return self.key

    def extract(self, infile: Path, outfile: Path):
        assert self.format == "turtle"
        output = self.program(infile)
        outfile.write_text(output)


@dataclass
class ETLTask:
    name: str
    input: Pattern
    output: Optional[Path]
    extractor: str
    optional: bool

    def to_dict(self):
        return {
            "name": self.name,
            "input": self.input,
            "output": self.output,
            "extractor": self.extractor,
            "optional": self.optional,
        }


@dataclass
class ETLConfig:
    """Configuration to run a pipeline"""

    extractors: dict[str, Extractor] = field(default_factory=dict)
    transformers: dict[str, Extractor] = field(default_factory=dict)
    pipeline: list[ETLTask] = field(default_factory=list)

    @staticmethod
    def parse(infile: Path):
        cfg = serde.yaml.deser(infile)
        assert cfg["version"] == 1

        pipeline = ETLConfig()
        for extractor in cfg["extractors"]:
            assert (
                extractor["name"] not in pipeline.extractors
            ), f"Extractor {extractor['name']} is duplicated"
            pipeline.extractors[extractor["name"]] = Extractor(
                name=extractor["name"],
                type=ExtractorType(extractor["type"]),
                args=extractor["args"],
                ext=extractor.get("ext", None),
            )

        names = set()
        for task in cfg["pipeline"]:
            name = task["name"]
            assert name not in names, f"Input {name} is duplicated"
            names.add(name)
            pipeline.pipeline.append(
                ETLTask(
                    name=name,
                    input=task["input"],
                    output=task.get("output", None),
                    extractor=task["extractor"],
                    optional=task.get("optional", False),
                )
            )

        return pipeline

    def to_dict(self):
        return {
            "extractors": [
                extractor.to_dict() for extractor in self.extractors.values()
            ],
            "pipeline": [task.to_dict() for task in self.pipeline],
        }
