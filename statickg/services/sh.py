from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Mapping, NotRequired, TypedDict

from tqdm import tqdm

from statickg.helper import logger_helper
from statickg.models.prelude import ETLOutput, RelPath, Repository
from statickg.services.interface import BaseFileWithCacheService, BaseService


class ShServiceConstructArgs(TypedDict):
    capture_output: bool
    verbose: NotRequired[int]


class ShServiceInvokeArgs(TypedDict):
    input: RelPath | list[RelPath]
    command: str
    optional: bool
    compute_missing_file_key: bool


class ShService(BaseFileWithCacheService[ShServiceInvokeArgs]):
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
        self.verbose = args.get("verbose", 1)

    def forward(
        self,
        repo: Repository,
        args: ShServiceInvokeArgs,
        tracker: ETLOutput,
    ):
        infiles = self.list_files(
            repo,
            args["input"],
            unique_filepath=True,
            optional=args.get("optional", False),
            compute_missing_file_key=args.get("compute_missing_file_key", True),
        )

        # now loop through the input files and invoke them.
        if self.capture_output:
            fn = subprocess.check_output
        else:
            fn = subprocess.check_call

        readable_ptns = self.get_readable_patterns(args["input"])
        with logger_helper(
            self.logger,
            1,
            extra_msg=f"matching {readable_ptns}",
        ) as log:
            for infile in tqdm(infiles, desc=readable_ptns, disable=self.verbose >= 2):
                cmd = args["command"].format(FILEPATH=str(infile.path))
                infile_ident = infile.get_path_ident()
                with self.cache.auto(
                    filepath=infile_ident,
                    key=cmd + ":" + infile.key,
                    outfile=None,
                ) as notfound:
                    if notfound:
                        fn(
                            cmd,
                            shell=True,
                        )
                    log(notfound, infile_ident)
