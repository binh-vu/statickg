from __future__ import annotations

import importlib
import sys
from pathlib import Path

import orjson
import serde.json
from hugedict.sqlite import SqliteDict, SqliteDictArgs, SqliteDictFieldType
from loguru import logger

from statickg.models import (
    ETLConfig,
    Extractor,
    ExtractorImpl,
    InputFile,
    ProcessStatus,
    Repository,
)


class StaticKG:

    def __init__(self, etl: ETLConfig, workdir: Path, data_dir: Repository):
        self.etl = etl
        self.data_dir = data_dir
        self.workdir = workdir.resolve()

        self.prepare_work_dir()

        dicts = SqliteDict.mul2(
            workdir / "etl.db",
            SqliteDictArgs(
                tablename="build",
                keytype=SqliteDictFieldType.str,
                ser_value=orjson.dumps,
                deser_value=orjson.loads,
            ),
            SqliteDictArgs(
                tablename="process",
                keytype=SqliteDictFieldType.str,
                ser_value=lambda x: orjson.dumps(x.to_dict()),
                deser_value=lambda x: ProcessStatus.from_dict(orjson.loads(x)),
            ),
        )
        self.build_cache: SqliteDict[str, list[str]] = dicts[0]
        self.etl_cache: SqliteDict[str, ProcessStatus] = dicts[1]

        self.extractors: dict[str, ExtractorImpl] = {}
        self.logger = logger.bind(name="statickg")
        self.logger.add(
            workdir / "logs/{time}.log",
            rotation="00:00",
            retention="30 days",
            diagnose=False,
        )

    def run(self):
        output_dir = self.workdir / "data"

        for task in self.etl.pipeline:
            if task.output is None:
                input_outdir = self.workdir / "data" / task.name
            else:
                input_outdir = self.workdir / "data" / task.output
                input_outdir.mkdir(parents=True, exist_ok=True)

            infiles = self.data_dir.glob(task.input)
            if not task.optional and len(infiles) == 0:
                raise ValueError(f"Input {task.name} is required but not found")

            prev_infiles = self.build_cache.get(f"task:{task.name}", [])

            # make sure that previously deleted files are removed
            self.remove_deleted_files(infiles, prev_infiles, input_outdir)
            # then update the files that are currently used
            self.build_cache[f"task:{task.name}"] = [f.relpath for f in infiles]

            extractor = self.etl.extractors[task.extractor]
            extractor_impl = self.load_extractor(extractor)

            # now loop through the input files and extract them.
            for infile in infiles:
                infile.key = infile.key + f":{extractor_impl.get_key()}"

                if task.output is None:
                    outfile = (output_dir / task.name / infile.relpath).parent
                    outfile.mkdir(parents=True, exist_ok=True)
                else:
                    outfile = output_dir / task.output

                if extractor.ext is not None:
                    outfile = outfile / f"{infile.path.stem}.{extractor.ext}"
                else:
                    outfile = outfile / infile.path.name

                rebuild = True
                if outfile.exists() and infile.relpath in self.etl_cache:
                    status = self.etl_cache[infile.relpath]
                    if status.key == infile.key and status.is_success:
                        rebuild = False

                if rebuild:
                    extractor_impl.extract(infile.path, outfile)
                    self.etl_cache[infile.relpath] = ProcessStatus(
                        key=infile.key, is_success=True
                    )

                    self.logger.debug("Extract {}", infile.relpath)
                else:
                    self.logger.debug("Skip {}", infile.relpath)

    def remove_deleted_files(
        self, newfiles: list[InputFile], oldfiles: list[str], basedir: Path
    ):
        newfile_paths = {f.relpath for f in newfiles}
        for oldfile in oldfiles:
            if oldfile not in newfile_paths:
                (basedir / oldfile).unlink()
                self.logger.debug("Remove {}", oldfile)

    def load_extractor(self, extractor: Extractor):
        if extractor.name not in self.extractors:
            self.extractors[extractor.name] = extractor.create(
                self.extractor_dir,
                self.etl_cache,
            )
        return self.extractors[extractor.name]

    def prepare_work_dir(self):
        """Prepare the working directory for the ETL process"""
        (self.workdir / "logs").mkdir(parents=True, exist_ok=True)
        cfgfile = self.workdir / "config.json"
        if cfgfile.exists():
            if serde.json.deser(cfgfile) != self.etl.to_dict():
                raise ValueError(
                    "The configuration file already exists and is different from the current configuration"
                )
        else:
            serde.json.ser(self.etl.to_dict(), cfgfile, indent=2)

        try:
            importlib.import_module("extractors")

            try:
                importlib.import_module("etl_extractors")
                raise ValueError(
                    "Existing a python package named etl_extractors, please uninstall it because it is reserved to store extractor programs"
                )
            except ModuleNotFoundError:
                self.extractor_dir = self.workdir / "etl_extractors"
        except ModuleNotFoundError:
            # we can use extractors as the name of the folder containing the extractors as it doesn't conflict with any existing
            # python packages
            self.extractor_dir = self.workdir / "extractors"

        self.extractor_dir.mkdir(parents=True, exist_ok=True)
        (self.extractor_dir / "__init__.py").touch(exist_ok=True)
        (self.workdir / "data").mkdir(parents=True, exist_ok=True)

        # so that python can find the extractors
        if str(self.workdir) not in sys.path:
            sys.path.insert(0, str(self.workdir))
