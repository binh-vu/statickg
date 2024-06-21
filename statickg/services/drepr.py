from __future__ import annotations

import hashlib
import importlib
import sys
from importlib.metadata import version
from pathlib import Path
from typing import Callable, Mapping, NotRequired, TypedDict

from drepr.main import convert
from tqdm import tqdm

from statickg.helper import import_func, logger_helper, remove_deleted_files
from statickg.models.prelude import ETLOutput, RelPath, Repository
from statickg.services.interface import BaseFileService, BaseService


class DReprServiceConstructArgs(TypedDict):
    path: RelPath | list[RelPath]
    format: str
    verbose: NotRequired[int]


class DReprServiceInvokeArgs(TypedDict):
    input: RelPath | list[RelPath]
    output: RelPath
    optional: NotRequired[bool]
    compute_missing_file_key: NotRequired[bool]


class DReprService(BaseFileService[DReprServiceInvokeArgs]):
    """
    D-REPR Service that is used to extract data from a file

    Args:
        name: name of the service
        workdir: working directory
        pylib_dir: directory where the python program is created
        args: arguments to the service
    """

    def __init__(
        self,
        name: str,
        workdir: Path,
        args: DReprServiceConstructArgs,
        services: Mapping[str, BaseService],
    ):
        super().__init__(name, workdir, args, services)
        pkgdir = self.setup(workdir)

        self.verbose = args.get("verbose", 1)
        self.format = args["format"]
        assert self.format in {"turtle"}, self.format
        self.extension = {"turtle": "ttl"}[self.format]
        self.drepr_version = version("drepr-v2").strip()

        if isinstance(args["path"], list):
            files = args["path"]
        else:
            files = [args["path"]]

        self.programs: dict[str, tuple[str, Callable]] = {}
        for file in files:
            filepath = file.get_path()
            outfile = pkgdir / f"{filepath.stem}.py"
            programkey = (
                f"drepr:{self.drepr_version}:"
                + hashlib.sha256(filepath.read_bytes()).hexdigest()
            )

            file_ident = file.get_ident()
            with self.cache.auto(
                filepath=file_ident,
                key=programkey,
                outfile=outfile,
            ) as notfound:
                if notfound:
                    try:
                        convert(repr=filepath, resources={}, progfile=outfile)
                    except:
                        self.logger.error(
                            "Error when generating program {}", file_ident
                        )
                        raise
                    self.logger.info("generate program {}", file_ident)
                else:
                    self.logger.info("reuse program {}", file_ident)

            self.programs[filepath.stem] = (
                programkey,
                import_func(f"{outfile.parent.name}.{outfile.stem}.main"),
            )

    def forward(
        self,
        repo: Repository,
        args: DReprServiceInvokeArgs,
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

        if len(self.programs) == 1:
            first_proram = next(iter(self.programs.values()))
        else:
            first_proram = None

        # now loop through the input files and extract them.
        readable_ptns = self.get_readable_patterns(args["input"])
        with logger_helper(
            self.logger,
            self.verbose,
            extra_msg=f"matching {readable_ptns}",
        ) as log:
            for infile in tqdm(infiles, desc=readable_ptns, disable=self.verbose >= 2):
                outfile = outdir / f"{infile.path.stem}.{self.extension}"

                if len(self.programs) == 1:
                    assert first_proram is not None
                    programkey, program = first_proram
                else:
                    programkey, program = self.programs[infile.path.stem]

                infile_ident = infile.get_path_ident()
                with self.cache.auto(
                    filepath=infile_ident,
                    key=programkey + ":" + infile.key,
                    outfile=outfile,
                ) as notfound:
                    if notfound:
                        try:
                            output = program(infile.path)
                        except:
                            self.logger.error("Error when processing {}", infile_ident)
                            raise
                        outfile.write_text(output)

                    log(notfound, infile_ident)

    def setup(self, workdir: Path):
        pkgname = "gen_programs"
        pkgdir = workdir / f"services/drepr/{pkgname}"

        try:
            m = importlib.import_module(pkgname)
            if Path(m.__path__[0]) != pkgdir:
                raise ValueError(
                    f"Existing a python package named {pkgname}, please uninstall it because it is reserved to store generated DREPR programs"
                )
        except ModuleNotFoundError:
            # we can use services as the name of the folder containing the services as it doesn't conflict with any existing
            # python packages
            pass

        pkgdir.mkdir(parents=True, exist_ok=True)
        (pkgdir / "__init__.py").touch(exist_ok=True)

        # add the package to the path
        if str(pkgdir.parent) not in sys.path:
            sys.path.insert(0, str(pkgdir.parent))

        return pkgdir
