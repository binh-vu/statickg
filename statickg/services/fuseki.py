from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Mapping, NotRequired, TypedDict

from tqdm import tqdm

from statickg.helper import get_latest_version, logger_helper
from statickg.models.input_file import ProcessStatus
from statickg.models.prelude import Change, ETLFileTracker, RelPath, Repository
from statickg.services.interface import BaseFileService, BaseService


class FusekiDataLoaderServiceConstructArgs(TypedDict):
    capture_output: bool
    batch_size: int
    verbose: NotRequired[int]


class FusekiDataLoaderServiceCmdArgs(TypedDict):
    clear: str
    insert: str


class FusekiDataLoaderServiceInvokeArgs(TypedDict):
    input: RelPath | list[RelPath]
    input_basedir: RelPath
    dbdir: RelPath
    command: str
    optional: bool


class FusekiDataLoaderService(BaseFileService[FusekiDataLoaderServiceInvokeArgs]):
    """A service that can load data to Fuseki incrementally.
    However, if a file is deleted or modified, it will reload the data from scratch."""

    def __init__(
        self,
        name: str,
        workdir: Path,
        args: FusekiDataLoaderServiceConstructArgs,
        services: Mapping[str, BaseService],
    ):
        super().__init__(name, workdir, args, services)
        self.capture_output = args.get("capture_output", False)
        self.verbose = args.get("verbose", 1)
        self.batch_size = args.get("batch_size", 10)

    def forward(
        self,
        repo: Repository,
        args: FusekiDataLoaderServiceInvokeArgs,
        tracker: ETLFileTracker,
    ):
        dbversion = get_latest_version(args["dbdir"].get_path() / "version-*")
        dbdir = args["dbdir"].get_path() / f"version-{dbversion:03d}"

        infiles = self.list_files(
            repo,
            args["input"],
            unique_filename=True,
            optional=args.get("optional", False),
            compute_missing_file_key=False,
        )
        prefix_key = f"cmd:{args['command']}|version:{dbversion}"

        can_load_incremental = (dbdir / "_SUCCESS").exists()
        if can_load_incremental:
            for infile in infiles:
                infile_ident = infile.get_path_ident()
                if infile_ident in self.cache.db:
                    status = self.cache.db[infile_ident]
                    if status.key == (prefix_key + "|" + infile.key):
                        if not status.is_success:
                            can_load_incremental = False
                            break
                    else:
                        # the key is different --> the file is modified
                        can_load_incremental = False
                        break

        if can_load_incremental:
            # filter out the files that are already loaded
            infiles = [
                infile
                for infile in infiles
                if infile.get_path_ident() not in self.cache.db
            ]
        else:
            # invalidate the cache.
            self.cache.db.clear()

            # we can reuse existing dbdir if it is not successful -- meaning that there is no Fuseki running on it
            if dbdir.exists() and not (dbdir / "_SUCCESS").exists():
                # clean up the directory
                shutil.rmtree(dbdir)
            else:
                # move to the next version and update the prefix key
                dbversion += 1
                prefix_key = f"cmd:{args['command']}|version:{dbversion}"

                # create a new directory for the new version
                dbdir = args["dbdir"].get_path() / f"version-{dbversion:03d}"
            dbdir.mkdir(exist_ok=True, parents=True)

        # now loop through the input files and invoke them.
        if self.capture_output:
            fn = subprocess.check_output
        else:
            fn = subprocess.check_call

        readable_ptns = self.get_readable_patterns(args["input"])
        input_basedir = args["input_basedir"].get_path()
        with logger_helper(
            self.logger,
            1,
            extra_msg=f"matching {readable_ptns}",
        ) as log:
            for i in tqdm(
                range(0, len(infiles), self.batch_size),
                desc=readable_ptns,
                disable=self.verbose >= 2,
            ):
                batch = infiles[i : i + self.batch_size]
                batch_ident = [file.get_path_ident() for file in batch]
                cmd = args["command"].format(
                    DB_DIR=dbdir,
                    FILES=" ".join(
                        [str(file.path.relative_to(input_basedir)) for file in batch]
                    ),
                )

                # mark the files as processing
                for infile, infile_ident in zip(batch, batch_ident):
                    self.cache.db[infile_ident] = ProcessStatus(
                        prefix_key + "|" + infile.key, is_success=False
                    )

                fn(cmd, shell=True)

                for infile, infile_ident in zip(batch, batch_ident):
                    self.cache.db[infile_ident] = ProcessStatus(
                        prefix_key + "|" + infile.key, is_success=True
                    )
                    log(True, infile_ident)

        # create a _SUCCESS file to indicate that the data is loaded successfully
        (dbdir / "_SUCCESS").touch()
