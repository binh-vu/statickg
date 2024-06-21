from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import serde.yaml

from statickg.models.input_file import BaseType, RefPathRef, RelPath, RelPathRefStr


@dataclass
class Service:
    name: str
    classpath: str
    args: dict

    def to_dict(self):
        return {
            "name": self.name,
            "classpath": self.classpath,
            "args": self.args,
        }


@dataclass
class ETLTask:
    service: str
    args: dict

    def to_dict(self):
        return {
            "name": self.service,
            "args": self.args,
        }


@dataclass
class ETLOutput:
    invoke_args: dict[str, list] = field(default_factory=dict)
    output: dict[str, list] = field(default_factory=dict)

    def track(self, service: str, args: Any, output: Any):
        if service not in self.invoke_args:
            self.invoke_args[service] = []
        if service not in self.output:
            self.output[service] = []
        self.invoke_args[service].append(args)
        self.output[service].append(output)


@dataclass
class ETLConfig:
    """Configuration to run a pipeline"""

    services: dict[str, Service] = field(default_factory=dict)
    pipeline: list[ETLTask] = field(default_factory=list)

    @staticmethod
    def parse(infile: Path, dirs: dict[BaseType, Path]):
        cfg = serde.yaml.deser(infile)
        assert cfg["version"] == 1

        # convert relative path
        cfg = ETLConfig._handle_path(
            cfg, {f"::{k.value}::": (k, v.resolve()) for k, v in dirs.items()}
        )
        assert isinstance(cfg, dict)

        pipeline = ETLConfig()
        for service in cfg["services"]:
            assert (
                service["name"] not in pipeline.services
            ), f"service {service['name']} is duplicated"

            pipeline.services[service["name"]] = Service(
                name=service["name"],
                classpath=service["classpath"],
                args=service.get("args", {}),
            )

        for task in cfg["pipeline"]:
            pipeline.pipeline.append(
                ETLTask(
                    service=task["service"],
                    args=task.get("args", {}),
                )
            )

        return pipeline

    @staticmethod
    def _handle_path(cfg: Any, dirs: dict[str, tuple[BaseType, Path]]):
        if isinstance(cfg, str):
            for k, v in dirs.items():
                if cfg.startswith(k):
                    return RelPath(basetype=v[0], basepath=v[1], relpath=cfg[len(k) :])

            # try to parse ref string
            refs = []
            for k, v in dirs.items():
                assert re.match(r"[a-zA-Z_:]+", k), f"Invalid base {k}"
                matches = list(re.finditer(f"({k})" + r"\{([^\}]*)\}", cfg))[::-1]
                for m in matches:
                    refs.append(
                        RefPathRef(
                            m.start(),
                            m.end(),
                            RelPath(basetype=v[0], basepath=v[1], relpath=m.group(2)),
                        )
                    )

            if len(refs) > 0:
                return RelPathRefStr(refs=refs, value=cfg)
            return cfg
        if isinstance(cfg, dict):
            for k, v in cfg.items():
                cfg[k] = ETLConfig._handle_path(v, dirs)
            return cfg
        if isinstance(cfg, list):
            return [ETLConfig._handle_path(v, dirs) for v in cfg]
        return cfg

    def to_dict(self):
        return {
            "services": [service.to_dict() for service in self.services.values()],
            "pipeline": [task.to_dict() for task in self.pipeline],
        }
