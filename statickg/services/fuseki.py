from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Iterable, Mapping, NotRequired, Sequence, TypedDict

import requests
from rdflib import Graph
from tqdm import tqdm

from statickg.helper import get_latest_version, logger_helper
from statickg.models.prelude import (
    Change,
    ETLFileTracker,
    InputFile,
    ProcessStatus,
    RelPath,
    RelPathRefStr,
    RelPathRefStrOrStr,
    Repository,
)
from statickg.services.interface import BaseFileService, BaseService
from statickg.services.version import VersionService, VersionServiceInvokeArgs


class FusekiEndpoint(TypedDict):
    update: str
    gsp: str


class FusekiDataLoaderServiceConstructArgs(TypedDict):
    capture_output: bool
    batch_size: int
    verbose: NotRequired[int]


class FusekiDataLoaderServiceInvokeArgs(TypedDict):
    input: RelPath | list[RelPath]
    input_basedir: RelPath
    dbdir: RelPath
    endpoint: FusekiEndpoint
    command: RelPathRefStrOrStr
    optional: bool
    replaceable_input: NotRequired[RelPath | list[RelPath]]


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
            compute_missing_file_key=True,
        )

        if "replaceable_input" not in args:
            replaceable_infiles = []
        else:
            replaceable_infiles = self.list_files(
                repo,
                args["replaceable_input"],
                unique_filename=True,
                optional=args.get("optional", False),
                compute_missing_file_key=True,
            )

        prefix_key = f"cmd:{args['command']}|version:{dbversion}"

        can_load_incremental = (dbdir / "_SUCCESS").exists()
        if can_load_incremental:
            if len(infiles) + len(replaceable_infiles) != len(self.cache.db):
                # delete files prevent incremental loading
                can_load_incremental = False
            else:
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
                    else:
                        can_load_incremental = False

        if not can_load_incremental:
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
            cmd = args["command"]
            if isinstance(cmd, RelPathRefStr):
                cmd = cmd.deref()

            # filter out the files that are already loaded
            filtered_infiles: list[InputFile] = []
            for infile in infiles:
                infile_ident = infile.get_path_ident()
                if infile_ident in self.cache.db:
                    log(False, infile_ident)
                else:
                    filtered_infiles.append(infile)
            infiles = filtered_infiles

            # before we load the data, we need to clear success marker
            (dbdir / "_SUCCESS").unlink(missing_ok=True)

            has_lock = (dbdir / "tdb.lock").exists()

            for i in tqdm(
                range(0, len(infiles), self.batch_size),
                desc=readable_ptns,
                disable=self.verbose >= 2,
            ):
                batch = infiles[i : i + self.batch_size]
                batch_ident = [file.get_path_ident() for file in batch]

                # mark the files as processing
                for infile, infile_ident in zip(batch, batch_ident):
                    self.cache.db[infile_ident] = ProcessStatus(
                        prefix_key + "|" + infile.key, is_success=False
                    )

                if has_lock:
                    # if there is a lock file, we need to use the api
                    for file in batch:
                        self.upload_file(args["endpoint"], file.path)
                else:
                    fn(
                        cmd.format(
                            DB_DIR=dbdir,
                            FILES=" ".join(
                                [
                                    str(file.path.relative_to(input_basedir))
                                    for file in batch
                                ]
                            ),
                        ),
                        shell=True,
                    )

                for infile, infile_ident in zip(batch, batch_ident):
                    self.cache.db[infile_ident] = ProcessStatus(
                        prefix_key + "|" + infile.key, is_success=True
                    )
                    log(True, infile_ident)

            # now load the replaceable files
            if "replaceable_input" in args:
                readable_ptns = self.get_readable_patterns(args["replaceable_input"])
            for infile in tqdm(
                replaceable_infiles, desc=readable_ptns, disable=self.verbose >= 2
            ):
                infile_ident = infile.get_path_ident()
                with self.cache.auto(
                    filepath=infile_ident,
                    key=prefix_key + "|" + infile.key,
                    outfile=None,
                ) as notfound:
                    if notfound:
                        if has_lock:
                            # we are writing to the endpoint
                            assert can_load_incremental

                            g = Graph()
                            g.parse(infile.path, format=self.detect_format(infile.path))
                            # remove URIs
                            self.remove_uris(
                                args["endpoint"],
                                (str(s) for s in g.subjects()),
                            )
                            # upload the files to endpoint
                            self.upload_file(args["endpoint"], infile.path)
                        else:
                            # do not have lock, we need to make sure we run the command for the first time
                            assert infile_ident not in self.cache.db

                            fn(
                                cmd.format(
                                    DB_DIR=dbdir,
                                    FILES=str(infile.path.relative_to(input_basedir)),
                                ),
                                shell=True,
                            )

        # create a _SUCCESS file to indicate that the data is loaded successfully
        (dbdir / "_SUCCESS").touch()

    def remove_uris(self, endpoint: FusekiEndpoint, uris: Iterable[str]):
        resp = requests.post(
            url=endpoint["update"],
            data={
                "update": "DELETE { ?s ?p ?o } WHERE { ?s ?p ?o VALUES ?s { %s } }"
                % " ".join(f"<{uri}>" for uri in uris)
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/sparql-results+json",  # Requesting JSON format
            },
            verify=False,  # Set to False to bypass SSL verification as per the '-k' in curl
        )
        assert resp.status_code == 200, (resp.status_code, resp.text)

    def upload_file(self, endpoint: FusekiEndpoint, file: Path):
        resp = requests.post(
            endpoint["gsp"],
            data=file.read_text(),
            headers={"Content-Type": f"text/{self.detect_format(file)}; charset=utf-8"},
            verify=False,
        )
        assert resp.status_code == 200, (resp.status_code, resp.text)

    def detect_format(self, file: Path):
        assert file.suffix == ".ttl", f"Only turtle files (.ttl) are supported: {file}"
        return "turtle"
