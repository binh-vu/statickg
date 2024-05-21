from __future__ import annotations

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
        self.workdir = workdir

        self.prepare_work_dir(workdir)

        self.build_cache, self.etl_cache = SqliteDict.mul2(
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

        self.extractors: dict[str, ExtractorImpl] = {}
        self.logger = logger.bind(name="statickg")
        self.logger.add(
            workdir / "logs/{time}.log", retention="30 days", diagnose=False
        )

    def extract(self):
        for input_name, input in self.etl.inputs.items():
            input_outdir = self.workdir / "data" / input_name
            input_outdir.mkdir(parents=True, exist_ok=True)

            infiles = self.data_dir.glob(input.path)
            prev_infiles = self.build_cache[f"input:{input_name}"]

            # make sure that previously deleted files are removed
            self.remove_deleted_files(infiles, prev_infiles, input_outdir)
            # then update the files that are currently used
            self.build_cache[f"input:{input_name}"] = [f.relpath for f in infiles]

            extractor = self.etl.extractors[input.extractor]
            extractor_impl = self.load_extractor(extractor)

            # now loop through the input files and extract them.
            for infile in infiles:
                outfile = input_outdir / infile.relpath
                outfile = outfile.parent / f"{outfile.stem}.{extractor.ext}"

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
                self.workdir / "statickg_extractors",
                self.etl_cache,
            )
        return self.extractors[extractor.name]

    def prepare_work_dir(self, workdir: Path):
        """Prepare the working directory for the ETL process"""
        (workdir / "logs").mkdir(parents=True, exist_ok=True)
        cfgfile = workdir / "config.json"
        if cfgfile.exists():
            if serde.json.deser(cfgfile) != self.etl.to_dict():
                raise ValueError(
                    "The configuration file already exists and is different from the current configuration"
                )
        else:
            serde.json.ser(self.etl.to_dict(), cfgfile, indent=2)

        (workdir / "statickg_extractors").mkdir(parents=True, exist_ok=True)
        (workdir / "statickg_extractors" / "__init__.py").touch()
        (workdir / "data").mkdir(parents=True, exist_ok=True)
