from __future__ import annotations

import http.client as httplib
import re
import shutil
import subprocess
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, NotRequired, Optional, Sequence, TypedDict
from urllib import request
from uuid import uuid4

import orjson
import requests
from rdflib import Graph
from tqdm import tqdm

from statickg.helper import find_available_port, get_latest_version, logger_helper
from statickg.models.prelude import (
    Change,
    ETLOutput,
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
    start: RelPathRefStrOrStr
    stop: RelPathRefStrOrStr
    find_by_id: str


class FusekiServiceConstructArgs(TypedDict):
    capture_output: bool
    batch_size: int
    verbose: NotRequired[int]


class FusekiLoadArgs(TypedDict):
    command: RelPathRefStrOrStr
    basedir: RelPath | str
    dbdir: RelPath | str
    optional: NotRequired[bool]


class FusekiDataLoaderServiceInvokeArgs(TypedDict):
    input: RelPath | list[RelPath]
    replaceable_input: NotRequired[RelPath | list[RelPath]]
    endpoint: FusekiEndpoint
    load: FusekiLoadArgs


@dataclass
class DBInfo:
    # command and version of the database -- together with the file key, it uniquely
    # identifies the file
    key: str
    version: int
    dir: Path
    hostname: Optional[str] = None

    def get_file_key(self, filekey: str) -> str:
        return self.key + "|" + filekey

    def is_valid(self) -> bool:
        return (self.dir / "_SUCCESS").exists()

    def invalidate(self):
        (self.dir / "_SUCCESS").unlink(missing_ok=True)

    def mark_valid(self):
        (self.dir / "_SUCCESS").touch()

    def has_running_service(self) -> bool:
        """Check if the directory has a running Fuseki service"""
        return self.hostname is not None

    def next(self) -> DBInfo:
        """Get next directory for the database"""
        return DBInfo(
            key=self.key,
            version=self.version + 1,
            dir=self.dir.parent / f"version-{self.version + 1:03d}",
        )

    @staticmethod
    def get_current_dbinfo(args: FusekiDataLoaderServiceInvokeArgs):
        dbdir = args["load"]["dbdir"]
        if isinstance(dbdir, str):
            dbdir = Path(dbdir)
        else:
            dbdir = dbdir.get_path()

        dbversion = get_latest_version(dbdir / "version-*")
        dbdir = dbdir / f"version-{dbversion:03d}"

        dbinfo = DBInfo(
            key=f"cmd:{args['load']['command']}|version:{dbversion}",
            version=dbversion,
            dir=dbdir,
        )

        # try to find the hostname
        output = (
            subprocess.check_output(
                args["endpoint"]["find_by_id"].format(ID=f"fuseki-{dbdir.name}"),
                shell=True,
            )
            .decode()
            .strip()
        )

        if output != "":
            m = re.match(r"(\d+\.\d+\.\d+\.\d+):(\d+)", output)
            if m is not None:
                ip, port = m.group(1), m.group(2)
                if ip == "0.0.0.0":
                    hostname = f"http://localhost:{port}"
                else:
                    hostname = f"http://{ip}:{port}"
            else:
                assert output.isdigit(), output
                hostname = f"http://localhost:{output}"
            dbinfo.hostname = hostname
        return dbinfo


class FusekiDataLoaderService(BaseFileService[FusekiDataLoaderServiceInvokeArgs]):
    """A service that can ensure that the Fuseki service is running with the latest data."""

    def __init__(
        self,
        name: str,
        workdir: Path,
        args: FusekiServiceConstructArgs,
        services: Mapping[str, BaseService],
    ):
        super().__init__(name, workdir, args, services)
        self.capture_output = args.get("capture_output", False)
        self.verbose = args.get("verbose", 1)
        self.batch_size = args.get("batch_size", 10)
        self.started_services = {}
        self.hostname = "http://localhost"

    def forward(
        self,
        repo: Repository,
        args: FusekiDataLoaderServiceInvokeArgs,
        tracker: ETLOutput,
    ):
        args = deepcopy(args)

        # --------------------------------------------------------------
        # get all input files
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

        # --------------------------------------------------------------
        # determine if we can load the data incrementally
        dbinfo = DBInfo.get_current_dbinfo(args)
        can_load_incremental = dbinfo.is_valid()
        if can_load_incremental:
            if len(infiles) + len(replaceable_infiles) != len(self.cache.db):
                # delete files prevent incremental loading
                can_load_incremental = False
            else:
                for infile in infiles:
                    infile_ident = infile.get_path_ident()
                    if infile_ident in self.cache.db:
                        status = self.cache.db[infile_ident]
                        if status.key == dbinfo.get_file_key(infile.key):
                            if not status.is_success:
                                can_load_incremental = False
                                break
                        else:
                            # the key is different --> the file is modified
                            can_load_incremental = False
                            break
                    else:
                        can_load_incremental = False

        # --------------------------------------------------------------
        # if we cannot load the data incrementally, we need to reload the data from scratch
        if not can_load_incremental:
            # invalidate the cache.
            self.cache.db.clear()

            if dbinfo.has_running_service():
                # we cannot reuse the existing dbdir because a Fuseki service is running on it
                # so we need to move to the next version
                dbinfo = dbinfo.next()
            else:
                # we can reuse existing dbdir, however, if it's invalid, we need to clean previous data
                if not dbinfo.is_valid():
                    # clean up the directory
                    shutil.rmtree(dbinfo.dir, ignore_errors=True)

        dbinfo.dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(
            "Loading data to {} (incremental = {})", dbinfo.dir, can_load_incremental
        )
        # --------------------------------------------------------------
        # now loop through the input files and invoke them.
        readable_ptns = self.get_readable_patterns(args["input"])
        with logger_helper(
            self.logger,
            1,
            extra_msg=f"matching {readable_ptns}",
        ) as log:
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
            dbinfo.invalidate()

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
                        dbinfo.get_file_key(infile.key), is_success=False
                    )

                # load the files
                self.load_files(args, dbinfo, False, batch)

                # mark the files as processed
                for infile, infile_ident in zip(batch, batch_ident):
                    self.cache.db[infile_ident] = ProcessStatus(
                        dbinfo.get_file_key(infile.key), is_success=True
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
                    key=dbinfo.get_file_key(infile.key),
                    outfile=None,
                ) as notfound:
                    if notfound:
                        self.load_files(args, dbinfo, True, [infile])

        # create a _SUCCESS file to indicate that the data is loaded successfully
        dbinfo.mark_valid()
        return dbinfo

    def start_fuseki(self, args: FusekiDataLoaderServiceInvokeArgs, dbinfo: DBInfo):
        if dbinfo.dir in self.started_services:
            return

        name = f"fuseki-{dbinfo.dir.name}"
        port = find_available_port(self.hostname, 3031)
        try:
            (subprocess.check_output if self.capture_output else subprocess.check_call)(
                self.get_start_command(args).format(
                    ID=name, PORT=str(port), DB_DIR=dbinfo.dir
                ),
                shell=True,
            )
        except:
            # sometime the process terminates but it leaves some trash behind -- stop and start again
            (subprocess.check_output if self.capture_output else subprocess.check_call)(
                self.get_stop_command(args).format(ID=name),
                shell=True,
            )
            (subprocess.check_output if self.capture_output else subprocess.check_call)(
                self.get_start_command(args).format(
                    ID=name, PORT=str(port), DB_DIR=dbinfo.dir
                ),
                shell=True,
            )

        self.started_services[dbinfo.dir] = (name, port)
        assert dbinfo.hostname is None
        dbinfo.hostname = f"{self.hostname}:{port}"
        self.logger.debug(
            "Started Fuseki service at {} serving {}", dbinfo.hostname, dbinfo.dir.name
        )

    def shutdown_fuseki(self, args: FusekiDataLoaderServiceInvokeArgs, dbinfo: DBInfo):
        if len(self.started_services) == 0:
            return

        # we should only have one service running at a time
        assert len(self.started_services) == 1
        assert dbinfo.dir in self.started_services
        (subprocess.check_output if self.capture_output else subprocess.check_call)(
            self.get_stop_command(args).format(ID=self.started_services[dbinfo.dir][0]),
            shell=True,
        )

        self.logger.debug(
            "Stopped Fuseki service at {}, which serves {}",
            dbinfo.hostname,
            dbinfo.dir.name,
        )
        self.started_services.pop(dbinfo.dir)
        dbinfo.hostname = None

    def load_files(
        self,
        args: FusekiDataLoaderServiceInvokeArgs,
        dbinfo: DBInfo,
        is_replaceable: bool,
        files: list[InputFile],
    ):
        # we have option to either update the files on disk directly or have to load them via Fuseki service
        update_graph = False
        if is_replaceable:
            assert len(files) == 1
            file = files[0]
            if file.get_path_ident() in self.cache.db:
                # the file has been loaded before --> we need to remove the URIs first
                update_graph = True

        if update_graph:
            # we need Fuseki service to remove the URIs first
            self.start_fuseki(args, dbinfo)
            assert dbinfo.hostname is not None
            for file in files:
                if file.get_path_ident() in self.cache.db:
                    self.remove_file(dbinfo.hostname, args["endpoint"], file.path)

        if dbinfo.has_running_service():
            # we cannot load the data directly to the database because the service is running
            # we need to upload the data to the endpoint.
            assert dbinfo.hostname is not None
            for file in files:
                self.upload_file(dbinfo.hostname, args["endpoint"], file.path)
        else:
            basedir = args["load"]["basedir"]
            if not isinstance(basedir, str):
                basedir = basedir.get_path()
            (subprocess.check_output if self.capture_output else subprocess.check_call)(
                self.get_load_command(args).format(
                    DB_DIR=dbinfo.dir,
                    FILES=" ".join(
                        [str(file.path.relative_to(basedir)) for file in files]
                    ),
                ),
                shell=True,
            )

    def upload_file(self, hostname: str, endpoint: FusekiEndpoint, file: Path):
        resp = requests.post(
            hostname + endpoint["gsp"],
            data=file.read_text(),
            headers={"Content-Type": f"text/{self.detect_format(file)}; charset=utf-8"},
            verify=False,
        )
        assert resp.status_code == 200, (resp.status_code, resp.text)

    def remove_file(self, hostname: str, endpoint: FusekiEndpoint, file: Path):
        g = Graph()
        g.parse(file, format=self.detect_format(file))
        resp = requests.post(
            url=hostname + endpoint["update"],
            data={
                "update": "DELETE { ?s ?p ?o } WHERE { ?s ?p ?o VALUES ?s { %s } }"
                % " ".join(f"<{str(s)}>" for s in g.subjects())
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/sparql-results+json",  # Requesting JSON format
            },
            verify=False,  # Set to False to bypass SSL verification as per the '-k' in curl
        )
        assert resp.status_code == 200, (resp.status_code, resp.text)

    def detect_format(self, file: Path):
        assert file.suffix == ".ttl", f"Only turtle files (.ttl) are supported: {file}"
        return "turtle"

    def get_load_command(self, args: FusekiDataLoaderServiceInvokeArgs):
        cmd = args["load"]["command"]
        if isinstance(cmd, RelPathRefStr):
            cmd = cmd.deref()
            # trick to avoid calling deref() again
            args["load"]["command"] = cmd
        return cmd

    def get_start_command(self, args: FusekiDataLoaderServiceInvokeArgs):
        cmd = args["endpoint"]["start"]
        if isinstance(cmd, RelPathRefStr):
            cmd = cmd.deref()
            # trick to avoid calling deref() again
            args["endpoint"]["start"] = cmd
        return cmd

    def get_stop_command(self, args: FusekiDataLoaderServiceInvokeArgs):
        cmd = args["endpoint"]["stop"]
        if isinstance(cmd, RelPathRefStr):
            cmd = cmd.deref()
            # trick to avoid calling deref() again
            args["endpoint"]["stop"] = cmd
        return cmd
