from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Mapping, TypedDict

from statickg.helper import logger_helper
from statickg.models.prelude import ETLFileTracker, RelPath, Repository
from statickg.services.interface import BaseFileService, BaseService


class ShServiceConstructArgs(TypedDict):
    capture_output: bool
    verbose: int


class ShServiceInvokeArgs(TypedDict):
    input: RelPath | list[RelPath]
    command: str
    optional: bool
    compute_missing_file_key: bool


class ShService(BaseFileService[ShServiceInvokeArgs]):
    """ """

    def __init__(
        self,
        name: str,
        workdir: Path,
        args: ShServiceConstructArgs,
        services: Mapping[str, BaseService],
    ):
        super().__init__(name, workdir, args, services)
        self.capture_output = args["capture_output"]

    def forward(
        self,
        repo: Repository,
        args: ShServiceInvokeArgs,
        tracker: ETLFileTracker,
    ):
        infiles = self.list_files(
            repo,
            args["input"],
            unique_filename=True,
            optional=args.get("optional", False),
            compute_missing_file_key=args.get("compute_missing_file_key", True),
        )

        # now loop through the input files and invoke them.
        if self.capture_output:
            fn = subprocess.check_output
        else:
            fn = subprocess.check_call

        with logger_helper(
            self.logger,
            1,
            extra_msg=f"matching {self.get_readable_patterns(args['input'])}",
        ) as log:
            for infile in infiles:
                with self.cache.auto(
                    filepath=infile.relpath,
                    key=infile.key,
                    outfile=None,
                ) as notfound:
                    if notfound:
                        fn(
                            args["command"].format(FILEPATH=str(infile.path)),
                            shell=True,
                        )
                    log(notfound, infile.relpath)
