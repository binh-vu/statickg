from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from copy import deepcopy
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Mapping, NotRequired, Optional, TypedDict

import serde.json
from timer import Timer

from statickg.helper import (
    find_available_port,
    get_latest_version,
    is_port_available,
    logger_helper,
    wait_till_port_available,
)
from statickg.models.prelude import (
    ETLOutput,
    InputFile,
    ProcessStatus,
    RelPath,
    RelPathRefStr,
    RelPathRefStrOrStr,
    Repository,
)
from statickg.services.interface import BaseFileWithCacheService, BaseService

DBINFO_METADATA_FILE = "_STATIC_KG_METADATA"


class DeploymentConfig(TypedDict):
    port: int


class DataLoaderServiceInvokeArgs(TypedDict):
    input: RelPath | list[RelPath]
    replaceable_input: NotRequired[RelPath | list[RelPath]]
    deploy: NotRequired[
        DeploymentConfig
    ]  # whether to deploy the service after loading the data


class DataLoaderServiceConstructArgs(TypedDict):
    capture_output: bool
    hostname: NotRequired[str]
    dbdir: RelPath | str
    # the command to start the service
    start_service: RelPathRefStrOrStr
    # the command to stop the service
    stop_service: RelPathRefStrOrStr
    # the command to load data into the service
    load_cmd: RelPathRefStrOrStr
    # the command to find the service by id
    find_by_id: str


class DataLoaderService(BaseFileWithCacheService[DataLoaderServiceConstructArgs]):
    """A data loader service that can ensure the current database is running with the latest data"""

    def __init__(
        self,
        name: str,
        workdir: Path,
        args: DataLoaderServiceConstructArgs,
        services: Mapping[str, BaseService],
    ):
        super().__init__(name, workdir, args, services)
        if isinstance(args["dbdir"], str):
            self.dbdir = Path(args["dbdir"])
        else:
            self.dbdir = args["dbdir"].get_path()

        self.capture_output = args.get("capture_output", False)
        self.hostname = args.get("hostname", "http://localhost")
        self.db_temp_port = int(os.environ.get("DB_TMP_PORT", "15524"))

    def forward(
        self, repo: Repository, args: DataLoaderServiceInvokeArgs, tracker: ETLOutput
    ):
        args = deepcopy(args)

        # --------------------------------------------------------------
        # get all input files
        infiles = self.list_files(
            repo,
            args["input"],
            unique_filepath=True,
            optional=args.get("optional", False),
            compute_missing_file_key=True,
        )

        if "replaceable_input" not in args:
            replaceable_infiles = []
        else:
            replaceable_infiles = self.list_files(
                repo,
                args["replaceable_input"],
                unique_filepath=True,
                optional=args.get("optional", False),
                compute_missing_file_key=True,
            )

        # --------------------------------------------------------------
        # determine if we can load the data incrementally
        dbinfo = self.get_current_dbinfo()
        can_load_incremental = dbinfo.is_valid()
        can_load_incremental_explanation = []

        if not can_load_incremental:
            can_load_incremental_explanation.append("the database is not valid")

        if can_load_incremental:
            prev_infile_idents = set(self.cache.db.keys())
            current_infile_idents = {file.get_path_ident() for file in infiles}.union(
                (file.get_path_ident() for file in replaceable_infiles)
            )

            if _tmp_removed_files := prev_infile_idents.difference(
                current_infile_idents
            ):
                # some files are removed
                can_load_incremental = False
                can_load_incremental_explanation.append(
                    "some files are removed (e.g., {file})".format(
                        file=next(iter(_tmp_removed_files))
                    )
                )
            else:
                for infile in infiles:
                    infile_ident = infile.get_path_ident()
                    if infile_ident in self.cache.db:
                        status = self.cache.db[infile_ident]
                        if status.key == dbinfo.get_file_key(infile.key):
                            if not status.is_success:
                                can_load_incremental = False
                                can_load_incremental_explanation.append(
                                    f"{infile_ident} is not successfully loaded (this shouldn't happen)"
                                )
                                break
                        else:
                            # the key is different --> the file is modified
                            can_load_incremental = False
                            can_load_incremental_explanation.append(
                                f"{infile_ident} is modified"
                            )
                            break

        # if we cannot load the data incrementally, we need to reload the data from scratch
        if not can_load_incremental:
            self.logger.info(
                "Cannot load the data incrementally. Reasons: {}",
                "\n".join(f"\t- {x}" for x in can_load_incremental_explanation),
            )

            if dbinfo.has_running_service():
                # we cannot reuse the existing dbdir because a database service is running on it
                # so we need to move to the next version
                dbinfo = dbinfo.next()
            else:
                # we can reuse existing dbdir, however, if it's invalid, we need to clean previous data
                if not dbinfo.is_valid():
                    # clean up the directory
                    shutil.rmtree(dbinfo.dir, ignore_errors=True)

            # invalidate the cache.
            self.cache.db.clear()

        dbinfo.dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(
            "Loading data to {} (incremental = {})", dbinfo.dir, can_load_incremental
        )

        # --------------------------------------------------------------
        # now loop through the input files and load them.
        start = time.time()
        # before we load the data, we need to clear success marker
        dbinfo.invalidate()
        try:
            readable_ptns = self.get_readable_patterns(args["input"])
            with logger_helper(
                self.logger,
                1,
                extra_msg=f"loading {readable_ptns}",
            ) as log:
                # filter out the files that are already loaded
                filtered_infiles: list[InputFile] = []
                for infile in infiles:
                    infile_ident = infile.get_path_ident()
                    if infile_ident in self.cache.db:
                        # we already checked the processing status
                        log(False, infile_ident)
                    else:
                        filtered_infiles.append(infile)
                infiles = filtered_infiles

                # mark the files as processing
                infile_idents = [file.get_path_ident() for file in infiles]
                for infile, infile_ident in zip(infiles, infile_idents):
                    self.cache.db[infile_ident] = ProcessStatus(
                        dbinfo.get_file_key(infile.key), is_success=False
                    )

                if len(infiles) > 0:
                    with Timer().watch_and_report(">>> load files"):
                        # load the files
                        self.load_files(args, dbinfo, infiles)

                # mark the files as processed
                for infile, infile_ident in zip(infiles, infile_idents):
                    self.cache.db[infile_ident] = ProcessStatus(
                        dbinfo.get_file_key(infile.key), is_success=True
                    )
                    log(True, infile_ident)

            # load the replaceable files
            if "replaceable_input" in args:
                readable_ptns = self.get_readable_patterns(args["replaceable_input"])
                with logger_helper(
                    self.logger,
                    1,
                    extra_msg=f"loading replaceable {readable_ptns}",
                ) as log:
                    filtered_replaceable_infiles = [
                        infile
                        for infile in replaceable_infiles
                        if not self.cache.has_cache(
                            infile.get_path_ident(), dbinfo.get_file_key(infile.key)
                        )
                    ]
                    filtered_replaceable_infile_idents = [
                        infile.get_path_ident()
                        for infile in filtered_replaceable_infiles
                    ]

                    for infile, infile_ident in zip(
                        filtered_replaceable_infiles, filtered_replaceable_infile_idents
                    ):
                        self.cache.db[infile_ident] = ProcessStatus(
                            dbinfo.get_file_key(infile.key), is_success=False
                        )

                    if len(filtered_replaceable_infiles) > 0:
                        with Timer().watch_and_report(">>> replace files"):
                            # replace the files
                            self.replace_files(
                                args, dbinfo, filtered_replaceable_infiles
                            )

                    for infile, infile_ident in zip(
                        filtered_replaceable_infiles, filtered_replaceable_infile_idents
                    ):
                        self.cache.db[infile_ident] = ProcessStatus(
                            dbinfo.get_file_key(infile.key), is_success=True
                        )
                        log(True, infile_ident)
        finally:
            # stop the service if it has been started by one of the load commands
            # but only if the service is started in a different port than the
            # desired deployed port
            # this command has no effect if the service is not started
            if "deploy" not in args or (
                dbinfo.endpoint is not None
                and args["deploy"]["port"] != dbinfo.endpoint.port
            ):
                self.stop_service(dbinfo)

        end = time.time()
        # create a _SUCCESS file to indicate that the data is loaded successfully
        dbinfo.mark_valid()

        self.logger.info(
            "Loading data to {} took {:.3f} seconds", dbinfo.dir, end - start
        )

        if "deploy" in args:
            port = args["deploy"]["port"]
            if dbinfo.endpoint is not None:
                if dbinfo.endpoint.port != port:
                    self.logger.error(
                        "The service is already running on a different port: {}",
                        dbinfo.endpoint,
                    )
                    raise Exception(
                        "The service is already running on a different port: {}".format(
                            dbinfo.endpoint
                        )
                    )
            else:
                if not is_port_available(self.hostname, port):
                    # stop all old services
                    for old_db in self.get_older_dbinfos(dbinfo):
                        self.stop_service(old_db)

                if not wait_till_port_available(self.hostname, port, timeout=10):
                    self.logger.error(
                        f"After stopping all services, port {port} is still not available."
                    )
                    raise Exception(
                        f"Another one started a service on port {port} that is not managed by this service"
                    )

            self.start_service(dbinfo, desired_port=port)

            # remove old databases
            for old_db in self.get_older_dbinfos(dbinfo):
                self.stop_service(old_db)
                shutil.rmtree(old_db.dir)

        return dbinfo

    def replace_files(
        self, args: DataLoaderServiceInvokeArgs, dbinfo: DBInfo, files: list[InputFile]
    ):
        """Replace the content of the files in the database. We expect the the entities in the files are the same, only the content is different."""
        raise NotImplementedError()

    def load_files(
        self,
        args: DataLoaderServiceInvokeArgs,
        dbinfo: DBInfo,
        files: list[InputFile],
    ):
        """Load files into the database."""
        raise NotImplementedError()

    def start_service(self, dbinfo: DBInfo, desired_port: Optional[int] = None):
        if dbinfo.endpoint is not None:
            # the service is running
            return

        # if the service is not started, hostname must be None
        assert dbinfo.endpoint is None, dbinfo

        name = self.get_db_service_id(dbinfo.dir)
        port = desired_port or find_available_port(self.hostname, self.db_temp_port)
        start_cmd = self.start_command.format(
            ID=name, PORT=str(port), DB_DIR=dbinfo.dir
        )
        try:
            self.logger.debug(
                "Starting service with:\n\t- ID = {}\n\t- Command = {}", name, start_cmd
            )
            (subprocess.check_output if self.capture_output else subprocess.check_call)(
                start_cmd,
                shell=True,
            )
        except:
            # sometime the process terminates but it leaves some trash behind -- stop and start again
            self._stop_service(name)
            self.logger.debug("(Retry) Starting service with ID = {}", name)
            (subprocess.check_output if self.capture_output else subprocess.check_call)(
                start_cmd,
                shell=True,
            )

        dbinfo.endpoint = self.find_running_service_by_id(name)
        if dbinfo.endpoint is None:
            raise Exception(f"Failed to start service with ID = {name}")
        self._wait_till_service_is_ready(dbinfo)
        self.logger.debug(
            "Started service at {} serving {}", dbinfo.endpoint, dbinfo.dir.name
        )

    def _wait_till_service_is_ready(self, dbinfo: DBInfo):
        """Wait until the service is ready"""
        raise NotImplementedError()

    def stop_service(self, dbinfo: DBInfo):
        if dbinfo.endpoint is None:
            # the service isn't running
            return

        assert dbinfo.endpoint is not None, dbinfo
        self._stop_service(self.get_db_service_id(dbinfo.dir))
        dbinfo.endpoint = None

    def _stop_service(self, id: str):
        self.logger.debug("Stopping service with ID = {}", id)
        (subprocess.check_output if self.capture_output else subprocess.check_call)(
            self.stop_command.format(ID=id),
            shell=True,
        )
        if self.find_running_service_by_id(id) is not None:
            raise Exception(f"Failed to stop service with ID = {id}")
        self.logger.debug("Stopped service with ID = {}", id)

    def find_running_service_by_id(self, id: str) -> Optional[Endpoint]:
        """Return the hostname of a running service by id."""
        output = (
            subprocess.check_output(
                self.args["find_by_id"].format(ID=id),
                shell=True,
            )
            .decode()
            .strip()
        )

        if output != "":
            m = re.search(r"(\d+\.\d+\.\d+\.\d+):(\d+)", output)
            if m is not None:
                ip, port = m.group(1), m.group(2)
                if ip == "0.0.0.0":
                    endpoint = Endpoint(hostname=f"http://localhost", port=int(port))
                else:
                    endpoint = Endpoint(hostname=f"http://{ip}", port=int(port))
            else:
                assert output.isdigit(), output
                endpoint = Endpoint(hostname=f"http://localhost", port=int(output))
            return endpoint
        return None

    @cached_property
    def load_command(self):
        cmd = self.args["load_cmd"]
        if isinstance(cmd, RelPathRefStr):
            cmd = cmd.deref()
        return cmd

    @cached_property
    def start_command(self):
        cmd = self.args["start_service"]
        if isinstance(cmd, RelPathRefStr):
            cmd = cmd.deref()
        return cmd

    @cached_property
    def stop_command(self):
        cmd = self.args["stop_service"]
        if isinstance(cmd, RelPathRefStr):
            cmd = cmd.deref()
        return cmd

    def get_current_dbinfo(self) -> DBInfo:
        dbversion = get_latest_version(self.dbdir / "version-*")
        dbdir = self.dbdir / f"version-{dbversion:03d}"

        if (dbdir / DBINFO_METADATA_FILE).exists():
            metadata = serde.json.deser(dbdir / DBINFO_METADATA_FILE)
            assert metadata["command"] == str(self.args["load_cmd"]), (
                metadata["command"],
                str(self.args["load_cmd"]),
            )
            assert metadata["version"] == dbversion
        else:
            # dbversion may not be 0. for example if we are building version 1
            # and we failed, the _METADATA file may not be created yet. It will
            # be created when we successfully create the database.
            dbdir.mkdir(parents=True, exist_ok=True)
            metadata = {
                "command": str(self.args["load_cmd"]),
                "version": dbversion,
            }
            serde.json.ser(
                metadata,
                dbdir / DBINFO_METADATA_FILE,
            )

        return DBInfo(
            command=metadata["command"],
            version=dbversion,
            dir=dbdir,
            endpoint=self.find_running_service_by_id(self.get_db_service_id(dbdir)),
        )

    def get_older_dbinfos(self, dbinfo: DBInfo) -> list[DBInfo]:
        """Get older versions of the database"""
        versions = []
        for i in range(dbinfo.version):
            dir = dbinfo.dir.parent / f"version-{i:03d}"
            if dir.exists():
                assert (dir / DBINFO_METADATA_FILE).exists()
                info = DBInfo(
                    command=serde.json.deser(dir / DBINFO_METADATA_FILE)["command"],
                    version=i,
                    dir=dir,
                    endpoint=self.find_running_service_by_id(
                        self.get_db_service_id(dir)
                    ),
                )
                versions.append(info)
        return versions

    def get_db_service_id(self, db_store_dir: Path) -> str:
        return f"dbloader-{db_store_dir.name}"


@dataclass
class Endpoint:
    hostname: str
    port: int

    def __str__(self):
        return f"{self.hostname}:{self.port}"


@dataclass
class DBInfo:
    # command and version of the database -- together with the file key, it uniquely
    # identifies the file
    command: str
    version: int
    dir: Path
    # it is not None if the database is running
    endpoint: Optional[Endpoint] = None

    @cached_property
    def key(self):
        return f"cmd:{self.command}|version:{self.version}"

    def get_file_key(self, filekey: str) -> str:
        return self.key + "|" + filekey

    def is_valid(self) -> bool:
        return (self.dir / "_SUCCESS").exists()

    def invalidate(self):
        (self.dir / "_SUCCESS").unlink(missing_ok=True)

    def mark_valid(self) -> None:
        if (self.dir / DBINFO_METADATA_FILE).exists():
            metadata = serde.json.deser(self.dir / DBINFO_METADATA_FILE)
            assert metadata["command"] == self.command
            assert metadata["version"] == self.version
        else:
            # when we mark a dbinfo as valid, we should have the metadata file
            serde.json.ser(
                {
                    "command": self.command,
                    "version": self.version,
                },
                self.dir / DBINFO_METADATA_FILE,
            )
        (self.dir / "_SUCCESS").touch()

    def has_running_service(self) -> bool:
        """Check if the directory has a running Fuseki service"""
        return self.endpoint is not None

    def next(self) -> DBInfo:
        """Get next directory for the database"""
        return DBInfo(
            command=self.command,
            version=self.version + 1,
            dir=self.dir.parent / f"version-{self.version + 1:03d}",
        )
