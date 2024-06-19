from __future__ import annotations

import importlib
import sys
from pathlib import Path

import serde.json
from loguru import logger

from statickg.helper import import_attr, json_ser
from statickg.models.prelude import (
    BaseType,
    ETLConfig,
    ETLOutput,
    GitRepository,
    Repository,
)
from statickg.services.interface import BaseService


class ETLPipelineRunner:

    def __init__(self, etl: ETLConfig, workdir: Path, repo: Repository):
        self.etl = etl
        self.repo = repo
        self.workdir = workdir.resolve()

        self.prepare_work_dir()

        self.services: dict[str, BaseService] = {}
        for name, service in etl.services.items():
            self.services[name] = import_attr(service.classpath)(
                name, self.workdir, service.args, self.services
            )

        self.logger = logger.bind(name="statickg")
        self.logger.add(
            workdir / "logs/{time}.log",
            rotation="00:00",
            retention="30 days",
            diagnose=False,
        )

    @staticmethod
    def from_config_file(
        cfg_file: Path,
        workdir: Path,
        repo: GitRepository,
        overwrite_config: bool = False,
    ):
        etl = ETLConfig.parse(
            cfg_file,
            {
                BaseType.CFG_DIR: cfg_file.parent,
                BaseType.REPO: repo.repo,
                BaseType.DATA_DIR: workdir / "data",
                BaseType.WORK_DIR: workdir,
                BaseType.DB_DIR: workdir / "databases",
            },
        )

        if overwrite_config:
            if (workdir / "config.json").exists():
                (workdir / "config.json").unlink()

        return ETLPipelineRunner(etl, workdir, repo)

    def __call__(self):
        output = ETLOutput()
        for task in self.etl.pipeline:
            output.track(
                self.etl.services[task.service].classpath,
                task.args,
                self.services[task.service](self.repo, task.args, output),
            )

    def prepare_work_dir(self):
        """Prepare the working directory for the ETL process"""
        (self.workdir / "logs").mkdir(parents=True, exist_ok=True)
        (self.workdir / "data").mkdir(parents=True, exist_ok=True)
        (self.workdir / "databases").mkdir(parents=True, exist_ok=True)

        cfgfile = self.workdir / "config.json"
        if cfgfile.exists():
            etljson = json_ser(self.etl.to_dict(), indent=2)
            if etljson != cfgfile.read_bytes():
                raise ValueError(
                    "The configuration file already exists and is different from the current configuration"
                )
        else:
            cfgfile.write_bytes(json_ser(self.etl.to_dict(), indent=2))
