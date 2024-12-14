from __future__ import annotations

import shutil
from pathlib import Path
from typing import TypedDict

from libactor.cache import cache
from tqdm import tqdm

from statickg.helper import FileSqliteBackend, logger_helper, remove_deleted_files
from statickg.models.file_and_path import InputFile
from statickg.models.prelude import ETLOutput, RelPath, Repository
from statickg.services.interface import BaseFileService


class CopyServiceInvokeArgs(TypedDict):
    input: RelPath | list[RelPath]
    output: RelPath
    optional: bool


class CopyService(BaseFileService[CopyServiceInvokeArgs]):

    def forward(
        self,
        repo: Repository,
        args: CopyServiceInvokeArgs,
        tracker: ETLOutput,
    ):
        infiles = self.list_files(
            repo,
            args["input"],
            unique_filepath=True,
            optional=args.get("optional", False),
            compute_missing_file_key=args.get("compute_missing_file_key", True),
        )
        outdir = args["output"].get_path()
        outdir.mkdir(parents=True, exist_ok=True)

        # detect and remove deleted files
        remove_deleted_files({file.path.name for file in infiles}, args["output"])

        # now loop through the input files and copy them
        copy_fn = CopyFn.get_instance(self.workdir).invoke
        for infile in tqdm(
            infiles,
            desc=f"Copying files {self.get_readable_patterns(args['input'])}",
        ):
            copy_fn(infile, outdir / infile.path.name)


class CopyFn:
    instances = {}

    def __init__(self, workdir: Path):
        self.workdir = workdir

    @staticmethod
    def get_instance(workdir: Path):
        if workdir not in CopyFn.instances:
            CopyFn.instances[workdir] = CopyFn(workdir)
        return CopyFn.instances[workdir]

    @cache(
        backend=FileSqliteBackend.factory(),
        cache_ser_args={
            "infile": lambda x: x.get_ident(),
        },
    )
    def invoke(self, infile: InputFile, outfile: Path):
        shutil.copy(infile.path, outfile)
        return outfile
