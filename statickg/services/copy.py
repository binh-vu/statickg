from __future__ import annotations

import shutil
from typing import TypedDict

from statickg.helper import logger_helper, remove_deleted_files
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
            unique_filename=True,
            optional=args.get("optional", False),
            compute_missing_file_key=args.get("compute_missing_file_key", True),
        )
        outdir = args["output"].get_path()
        outdir.mkdir(parents=True, exist_ok=True)

        # detect and remove deleted files
        remove_deleted_files(infiles, args["output"])

        # now loop through the input files and copy them
        with logger_helper(
            self.logger,
            1,
            extra_msg=f"matching {self.get_readable_patterns(args['input'])}",
        ) as log:
            for infile in infiles:
                outfile = outdir / infile.path.name

                infile_ident = infile.get_path_ident()
                with self.cache.auto(
                    filepath=infile_ident,
                    key=infile.key,
                    outfile=outfile,
                ) as notfound:
                    if notfound:
                        shutil.copy(infile.path, outfile)
                    log(notfound, infile_ident)
