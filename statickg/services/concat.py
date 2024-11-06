from __future__ import annotations

from typing import TypedDict

from statickg.models.etl import ETLOutput
from statickg.models.file_and_path import RelPath
from statickg.models.repository import Repository
from statickg.services.interface import BaseFileService


class ConcatTTLServiceInvokeArgs(TypedDict):
    input: RelPath | list[RelPath]
    output: RelPath
    optional: bool


class ConcatTTLService(BaseFileService[ConcatTTLServiceInvokeArgs]):

    def forward(
        self, repo: Repository, args: ConcatTTLServiceInvokeArgs, tracker: ETLOutput
    ):
        infiles = self.list_files(
            repo,
            args["input"],
            unique_filepath=False,
            optional=args.get("optional", False),
            compute_missing_file_key=False,
        )

        outfile = args["output"].get_path()
        outfile.parent.mkdir(parents=True, exist_ok=True)

        prefixes = set()
        lines = []
        for infile in infiles:
            with open(infile.path, "r") as f:
                for line in f:
                    if line.startswith("@prefix"):
                        prefixes.add(line.strip())
                    else:
                        lines.append(line)
            lines.append("\n")

        with open(outfile, "w") as f:
            for prefix in prefixes:
                f.write(prefix + "\n")
            for line in lines:
                f.write(line)
