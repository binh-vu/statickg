from __future__ import annotations

import os
import pickle
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, NotRequired, TypedDict

import serde.json
import xxhash
from joblib import Parallel, delayed
from libactor.cache import SqliteBackend, cache
from tqdm import tqdm

from statickg.helper import FileSqliteBackend, get_classpath, remove_deleted_files
from statickg.models.prelude import ETLOutput, InputFile, RelPath
from statickg.models.repository import Repository
from statickg.services.interface import BaseFileService, BaseService
from statickg.services.split import HashSplitService, read_file, write_file


class HashFilterServiceConstructArgs(TypedDict):
    verbose: NotRequired[int]
    parallel: NotRequired[bool]


class HashFilterServiceInvokeArgs(TypedDict):
    key_prop: str | list[str]
    all_output: RelPath
    filter_output: RelPath
    output: RelPath


class HashFilterService(BaseService[HashFilterServiceInvokeArgs]):

    def __init__(
        self,
        name: str,
        workdir: Path,
        args: HashFilterServiceConstructArgs,
        services: Mapping[str, BaseService],
    ):
        self.name = name
        self.workdir = workdir
        self.services = services
        self.verbose = args.get("verbose", 1)
        self.parallel = args.get("parallel", True)
        self.parallel_executor = Parallel(n_jobs=-1, return_as="generator_unordered")

    def forward(
        self, repo: Repository, args: HashFilterServiceInvokeArgs, tracker: ETLOutput
    ):
        hashsplit_service = get_classpath(HashSplitService)

        assert len(tracker.invoke_args[hashsplit_service]) == 2
        (all_idx,) = [
            idx
            for idx, split_args in enumerate(tracker.invoke_args[hashsplit_service])
            if split_args["output"]["base"] == args["all_output"]
        ]
        (filter_idx,) = [
            idx
            for idx, split_args in enumerate(tracker.invoke_args[hashsplit_service])
            if split_args["output"]["base"] == args["filter_output"]
        ]
        assert all_idx != filter_idx

        all_output: dict[str, list[InputFile]] = tracker.output[hashsplit_service][
            all_idx
        ]
        filter_output: dict[str, list[InputFile]] = tracker.output[hashsplit_service][
            filter_idx
        ]

        outdir_base = args["output"]
        outdir_path = outdir_base.get_path()
        outdir_path.mkdir(parents=True, exist_ok=True)

        key_prop = args["key_prop"]

        jobs = []
        # they should be in the group, so we can just loop through them and apply filtering
        for bucket, files in all_output.items():
            jobs.append((bucket, filter_output.get(bucket, []), files))

        if self.parallel:
            it: Iterable = self.parallel_executor(
                delayed(filter_file)(
                    self.workdir, bucket, outdir_base, key_prop, filter_files, files
                )
                for bucket, filter_files, files in jobs
            )  # type: ignore
        else:
            it: Iterable = (
                filter_file(
                    self.workdir, bucket, outdir_base, key_prop, filter_files, files
                )
                for bucket, filter_files, files in jobs
            )

        for tmp in tqdm(
            it, total=len(jobs), desc="Filter files", disable=self.verbose != 1
        ):
            pass


def filter_file(
    workdir: Path,
    bucket: str,
    outdir: RelPath,
    key_prop: str | list[str],
    filter_files: list[InputFile],
    files: list[InputFile],
):
    # read the filter files
    keys = set()

    (outdir.get_path() / bucket).mkdir(parents=True, exist_ok=True)
    # we do not skip empty files -- so this code should be fine
    remove_deleted_files({file.path.name for file in files}, outdir / bucket)

    if isinstance(key_prop, str):
        for file in filter_files:
            keys.update((record[key_prop] for record in read_file(file.path)))

        filter_fn = FilterFn.get_instance(workdir)
        for file in files:
            filter_fn.filter(bucket, outdir, key_prop, filter_files, file, keys)
    else:
        for file in filter_files:
            keys.update(
                tuple(record[prop] for prop in key_prop)
                for record in read_file(file.path)
            )

        filter_fn = FilterFn.get_instance(workdir)
        for file in files:
            filter_fn.filter(bucket, outdir, key_prop, filter_files, file, keys)


class FilterFn:
    instances = {}

    def __init__(self, workdir: Path):
        self.workdir = workdir

    @staticmethod
    def get_instance(workdir: Path):
        if workdir not in FilterFn.instances:
            FilterFn.instances[workdir] = FilterFn(workdir)
        return FilterFn.instances[workdir]

    @cache(
        backend=FileSqliteBackend.factory(),
        cache_args=["bucket", "outdir", "key_prop", "filter_files", "file"],
        cache_ser_args={
            "outdir": lambda x: x.get_ident(),
            "key_prop": lambda x: x,
            "filter_files": lambda files: "\n".join(
                file.get_ident() for file in sorted(files)
            ),
            "file": lambda x: x.get_ident(),
        },  # type: ignore
    )
    def filter(
        self,
        bucket: str,
        outdir: RelPath,
        key_prop: str | list[str],
        filter_files: list[InputFile],
        file: InputFile,
        filter_keys: set[str],
    ):
        outfile = outdir.get_path() / bucket / file.path.name

        if len(filter_files) == 0:
            shutil.copy(file.path, outfile)
            return outfile

        old_records = read_file(file.path)
        records = [
            r
            for r in old_records
            if tuple(r[prop] for prop in key_prop) not in filter_keys
        ]

        if len(records) != len(old_records):
            write_file(records, outfile)
        else:
            shutil.copy(file.path, outfile)

        return outfile
