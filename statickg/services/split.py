from __future__ import annotations

import hashlib
import pickle
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Mapping, NotRequired, TypedDict

import serde.json
import xxhash
from joblib import Parallel, delayed
from libactor.cache import SqliteBackend, cache
from tqdm import tqdm

from statickg.models.etl import ETLOutput
from statickg.models.file_and_path import FormatOutputPath, InputFile, RelPath
from statickg.models.repository import Repository
from statickg.services.interface import BaseFileService, BaseService


class HashSplitServiceConstructArgs(TypedDict):
    verbose: NotRequired[int]
    parallel: NotRequired[bool]


class HashSplitServiceInvokeArgs(TypedDict):
    key_prop: str | list[str]
    input: RelPath | list[RelPath]
    output: FormatOutputPath
    num_buckets: NotRequired[int]
    optional: NotRequired[bool]
    compute_missing_file_key: NotRequired[bool]


class HashSplitService(BaseFileService[HashSplitServiceInvokeArgs]):
    """A service that split records of a file based on the hash of a record's field.

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
        args: HashSplitServiceConstructArgs,
        services: Mapping[str, BaseService],
    ):
        super().__init__(name, workdir, args, services)
        self.verbose = args.get("verbose", 1)
        self.parallel = args.get("parallel", True)
        self.parallel_executor = Parallel(n_jobs=-1, return_as="generator_unordered")

    def forward(
        self,
        repo: Repository,
        args: HashSplitServiceInvokeArgs,
        tracker: ETLOutput,
    ) -> dict[str, list[InputFile]]:
        infiles = self.list_files(
            repo,
            args["input"],
            unique_filepath=True,
            optional=args.get("optional", False),
            compute_missing_file_key=args.get("compute_missing_file_key", True),
        )
        outdir_base = args["output"]["base"]
        outdir_fmt = args["output"]["format"]
        outdir_path = outdir_base.get_path()
        outdir_path.mkdir(parents=True, exist_ok=True)

        key_prop = args["key_prop"]
        num_buckets = args.get("num_buckets", 1024)

        # split records into buckets
        jobs = []
        for infile in infiles:
            jobs.append((infile, key_prop, num_buckets))

        if self.parallel:
            it: Iterable[list[InputFile]] = self.parallel_executor(
                delayed(split_file)(
                    self.workdir, file, outdir_base, outdir_fmt, key_prop, num_buckets
                )
                for file, key_prop, num_buckets in jobs
            )  # type: ignore
        else:
            it: Iterable[list[InputFile]] = (
                split_file(
                    self.workdir, file, outdir_base, outdir_fmt, key_prop, num_buckets
                )
                for file, key_prop, num_buckets in jobs
            )

        # get list of all output files and remove unknown files
        outfiles = set()
        output = defaultdict(list)
        for tmp in tqdm(
            it, total=len(jobs), desc="Splitting files", disable=self.verbose != 1
        ):
            for outfile in tmp:
                tmp = outfile.path.relative_to(outdir_path)
                assert tmp not in outfiles
                outfiles.add(tmp)
                output[str(tmp.parent)].append(outfile)

        for x in outdir_path.glob("**/*.json"):
            if x.relative_to(outdir_path) not in outfiles:
                # remove unknown files
                x.unlink()

        return dict(output)


def split_file(workdir, file, outdir_base, outdir_fmt, key_prop, num_buckets):
    return SplitFn.get_instance(workdir).split_file(
        file, outdir_base, outdir_fmt, key_prop, num_buckets
    )


class SplitFn:
    instances = {}

    def __init__(self, workdir: Path):
        self.workdir = workdir

    @staticmethod
    def get_instance(workdir: Path):
        if workdir not in SplitFn.instances:
            SplitFn.instances[workdir] = SplitFn(workdir)
        return SplitFn.instances[workdir]

    @cache(
        backend=lambda slf, fn, arghelper: SqliteBackend(
            func=fn, ser=pickle.dumps, deser=pickle.loads, dbdir=slf.workdir
        ),
        cache_ser_args={
            "infile": lambda x: x.get_ident(),
            "outdir": lambda x: x.get_ident(),
            "key_prop": lambda x: x,
        },  # type: ignore
    )
    def split_file(
        self,
        infile: InputFile,
        outdir: RelPath,
        outdir_fmt: str,
        key_prop: str | tuple[str, ...],
        num_buckets: int,
    ) -> list[InputFile]:
        """Split a file into multiple buckets based on the hash of a record's field.

        This function returns the list of output files' relative paths.
        """
        records = read_file(infile.path)
        buckets = [[] for _ in range(num_buckets)]

        if isinstance(key_prop, str):
            for record in records:
                key = record[key_prop]
                bucketno = xxhash.xxh32(key).intdigest() % num_buckets
                buckets[bucketno].append(record)
        else:
            for record in records:
                key = "---".join(str(record[x]) for x in key_prop)
                bucketno = xxhash.xxh32(key).intdigest() % num_buckets
                buckets[bucketno].append(record)

        # write the buckets and return the buckets id
        outfiles: list[InputFile] = []
        for bucketno, bucket in enumerate(buckets):
            if len(bucket) == 0:
                continue
            outfile_relpath = outdir / outdir_fmt.format(
                fileparent=infile.path.parent.name,
                filegrandparent=infile.path.parent.parent.name,
                bucketno=bucketno,
                filename=f"{infile.path.stem}.json",
            )
            outfile_path = outfile_relpath.get_path()
            outfile_path.parent.mkdir(parents=True, exist_ok=True)
            serde.json.ser(bucket, outfile_path)

            outfiles.append(
                InputFile(
                    basetype=outfile_relpath.basetype,
                    key=hashlib.sha256(outfile_path.read_bytes()).hexdigest(),
                    relpath=outfile_relpath.relpath,
                    path=outfile_path,
                )
            )

        return outfiles


def read_file(file: Path):
    if file.suffix == ".json":
        records = serde.json.deser(file)
        assert isinstance(records, list)
    else:
        raise NotImplementedError(file.suffix)

    return records


def write_file(data: list, file: Path):
    if file.suffix == ".json":
        serde.json.ser(data, file)
    else:
        raise NotImplementedError(file.suffix)
