from __future__ import annotations

import importlib
import sys
from importlib.metadata import version
from pathlib import Path
from typing import Callable, Iterable, Mapping, NotRequired, TypeAlias, TypedDict

from drepr.main import convert
from joblib import Parallel, delayed
from libactor.cache import cache
from tqdm import tqdm

from statickg.helper import FileSqliteBackend, import_func
from statickg.models.file_and_path import InputFile
from statickg.models.prelude import ETLOutput, RelPath, Repository
from statickg.services.interface import BaseFileService, BaseService
from statickg.services.split import FormatOutputPath


class DReprServiceConstructArgs(TypedDict):
    path: RelPath | list[RelPath]
    format: str
    verbose: NotRequired[int]
    parallel: NotRequired[bool]


class DReprServiceInvokeArgs(TypedDict):
    input: RelPath | list[RelPath]
    output: RelPath | FormatOutputPath
    optional: NotRequired[bool]
    compute_missing_file_key: NotRequired[bool]


FORWARD_EXEC_JOB_RETURN_TYPE: TypeAlias = tuple[str, str]


class DReprService(BaseFileService[DReprServiceInvokeArgs]):
    """
    D-REPR Service that is used to extract data from a file

    Args:
        name: name of the service
        workdir: working directory
        args: arguments to the service
        services: a dictionary of services
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
        self.parallel = args.get("parallel", True)
        self.parallel_executor = Parallel(n_jobs=-1, return_as="generator_unordered")

        if isinstance(args["path"], list):
            files = args["path"]
        else:
            files = [args["path"]]

        self.programs: dict[str, tuple[str, str]] = {}
        for file in files:
            infile = InputFile.from_relpath(file)
            outfile = pkgdir / f"{infile.path.stem}.py"
            self.gen_program(
                self.drepr_version,
                repr_file=infile,
                prog_file=outfile,
            )
            programkey = f"drepr:{self.drepr_version}:{infile.key}"
            assert infile.path.stem not in self.programs
            self.programs[infile.path.stem] = (
                programkey,
                f"{outfile.parent.name}.{outfile.stem}.main",
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
            unique_filepath=True,
            optional=args.get("optional", False),
            compute_missing_file_key=args.get("compute_missing_file_key", True),
        )

        args_output = args["output"]
        if isinstance(args_output, RelPath):
            outdir = args_output.get_path()
            outdir.mkdir(parents=True, exist_ok=True)
            outdir_filename_fmt = "{filestem}.{fileext}"
        else:
            outdir = args_output["base"].get_path()
            outdir.mkdir(parents=True, exist_ok=True)
            outdir_filename_fmt = args_output["format"]

        if len(self.programs) == 1:
            first_proram = next(iter(self.programs.values()))
        else:
            first_proram = None

        # now loop through the input files and extract them.
        readable_ptns = self.get_readable_patterns(args["input"])
        jobs = []
        for infile in infiles:
            outfile = outdir / outdir_filename_fmt.format(
                fileparent=infile.path.parent.name,
                filegrandparent=infile.path.parent.parent.name,
                filestem=infile.path.stem,
                fileext=self.extension,
            )
            outfile.parent.mkdir(parents=True, exist_ok=True)

            if len(self.programs) == 1:
                assert first_proram is not None
                program_key, program_path = first_proram
            else:
                program_key, program_path = self.programs[infile.path.stem]

            jobs.append((program_key, program_path, infile, outfile))

        if self.parallel:
            it: Iterable = self.parallel_executor(
                delayed(drepr_exec)(
                    self.workdir, program_key, program_path, infile, outfile
                )
                for program_key, program_path, infile, outfile in jobs
            )
        else:
            it: Iterable = (
                drepr_exec(self.workdir, program_key, program_path, infile, outfile)
                for program_key, program_path, infile, outfile in jobs
            )

        outfiles = set()
        for outfile in tqdm(
            it, total=len(jobs), desc=readable_ptns, disable=self.verbose < 1
        ):
            outfiles.add(outfile.relative_to(outdir))

        self.remove_unknown_files(outfiles, outdir)

    def setup(self, workdir: Path):
        pkgname = "gen_programs"
        pkgdir = workdir / pkgname

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

    @cache(
        backend=FileSqliteBackend.factory(verbose="DREPR-Gen-Program"),
        cache_ser_args={
            "repr_file": lambda x: x.get_ident(),
        },
    )
    def gen_program(self, drepr_version: str, repr_file: InputFile, prog_file: Path):
        convert(repr=repr_file.path, resources={}, progfile=prog_file)
        return prog_file


def drepr_exec(
    workdir: Path, program_key: str, program_path: str, infile: InputFile, outfile: Path
) -> Path:
    return DReprFn.get_instance(workdir, program_key, program_path).exec(
        infile, outfile
    )


class DReprFn:

    instances = {}

    def __init__(self, workdir: Path, program_key: str, program_path: str):
        self.workdir = workdir
        self.program: tuple[str, Callable] = (
            program_key,
            import_func(program_path),
        )

    @staticmethod
    def get_instance(workdir: Path, program_key: str, program_path: str):
        key = (workdir, program_key, program_path)
        if key not in DReprFn.instances:
            DReprFn.instances[key] = DReprFn(workdir, program_key, program_path)
        return DReprFn.instances[key]

    @cache(
        backend=FileSqliteBackend.factory(),
        cache_ser_args={
            "infile": lambda x: x.get_ident(),
        },
    )
    def exec(self, infile: InputFile, outfile: Path):
        try:
            output = self.program[1](infile.path)
        except Exception as e:
            raise Exception(f"Error when processing {infile.path}") from e

        outfile.write_text(output)
        return outfile
