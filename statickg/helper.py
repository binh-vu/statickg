from __future__ import annotations

import glob
import importlib
import re
import socket
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Optional, Type

import orjson
from hugedict.sqlite import SqliteDict
from loguru import logger

from statickg.models.input_file import InputFile, ProcessStatus, RelPath, RelPathRefStr

TYPE_ALIASES = {"typing.List": "list", "typing.Dict": "dict", "typing.Set": "set"}


def get_classpath(type: Type | Callable) -> str:
    if type.__module__ == "builtins":
        return type.__qualname__

    if hasattr(type, "__qualname__"):
        return type.__module__ + "." + type.__qualname__

    # typically a class from the typing module
    if hasattr(type, "_name") and type._name is not None:
        path = type.__module__ + "." + type._name
        if path in TYPE_ALIASES:
            path = TYPE_ALIASES[path]
    elif hasattr(type, "__origin__") and hasattr(type.__origin__, "_name"):
        # found one case which is typing.Union
        path = type.__module__ + "." + type.__origin__._name
    else:
        raise NotImplementedError(type)

    return path


def import_func(func_ident: str) -> Callable:
    """Import function from string, e.g., sm.misc.funcs.import_func"""
    lst = func_ident.rsplit(".", 2)
    if len(lst) == 2:
        module, func = lst
        cls = None
    else:
        module, cls, func = lst
        try:
            importlib.import_module(module + "." + cls)
            module = module + "." + cls
            cls = None
        except ModuleNotFoundError as e:
            if e.name == (module + "." + cls):
                pass
            else:
                raise

    module = importlib.import_module(module)
    if cls is not None:
        module = getattr(module, cls)

    return getattr(module, func)


def import_attr(attr_ident: str):
    lst = attr_ident.rsplit(".", 1)
    module, cls = lst
    module = importlib.import_module(module)
    return getattr(module, cls)


def get_latest_version(file_pattern: str | Path) -> int:
    """Assuming the file pattern select list of files tagged with an integer version for every run, this
    function return the latest version number that you can use to name your next run.

    For example:
    1. If your pattern matches folders: version_1, version_5, version_6, this function will return 6.
    2. If your pattern does not match anything, return 0
    """
    files = [Path(file) for file in sorted(glob.glob(str(file_pattern)))]
    if len(files) == 0:
        return 0

    versions: list[int] = []
    for file in files:
        match = re.match(r"[^0-9]*(\d+)[^0-9]*", file.name)
        if match is None:
            raise Exception("Invalid naming")
        versions.append(int(match.group(1)))

    return sorted(versions)[-1]


def json_ser(obj: dict, indent: int = 0) -> bytes:
    if indent == 0:
        option = orjson.OPT_PASSTHROUGH_DATACLASS
    else:
        option = orjson.OPT_INDENT_2 | orjson.OPT_PASSTHROUGH_DATACLASS
    return orjson.dumps(obj, default=json_ser_default_object, option=option)


def json_ser_default_object(obj: Any):
    if isinstance(obj, RelPath):
        return obj.get_ident()
    if isinstance(obj, RelPathRefStr):
        return obj.value
    raise TypeError


def remove_deleted_files(newfiles: list[InputFile], outdir: RelPath):
    new_filenames = {file.path.stem for file in newfiles}
    for file in outdir.get_path().iterdir():
        if file.is_file() and file.stem not in new_filenames:
            file.unlink()
            logger.info("Remove deleted file {}", file)


class CacheProcess:
    def __init__(self, dbpath: Path):
        dbpath.parent.mkdir(parents=True, exist_ok=True)
        self.db = SqliteDict.str(
            dbpath,
            ser_value=lambda x: orjson.dumps(x.to_dict()),
            deser_value=lambda x: ProcessStatus.from_dict(orjson.loads(x)),
        )

    @contextmanager
    def auto(self, filepath: str, key: str, outfile: Optional[Path] = None):
        notfound = True
        if (outfile is None or outfile.exists()) and filepath in self.db:
            status = self.db[filepath]
            if status.key == key and status.is_success:
                notfound = False

        yield notfound

        if notfound:
            self.db[filepath] = ProcessStatus(key, is_success=True)


@contextmanager
def logger_helper(alogger, verbose: int, extra_msg: str = ""):
    nprocess = 0
    nskip = 0

    def log(notfound: bool, filepath: str):
        nonlocal nprocess, nskip

        if verbose == 0:
            # no logging
            return

        if verbose == 1:
            # only log aggregated information
            if notfound:
                nprocess += 1
            else:
                nskip += 1
            return

        if verbose == 2:
            # show aggregated info for skip
            if notfound:
                alogger.info("Process {}", filepath)
            else:
                nskip += 1
            return

        assert verbose >= 3
        if notfound:
            alogger.info("Process {}", filepath)
        else:
            alogger.info("Skip {}", filepath)

    yield log

    if verbose == 0:
        # no logging
        return

    if verbose == 1:
        # only log aggregated information
        alogger.info("Process {} and skip {} files {}", nprocess, nskip, extra_msg)
        return

    if verbose == 2:
        # show aggregated info for skip
        alogger.info("Skip {} files {}", nskip, extra_msg)
        return

    assert verbose >= 3
    return  # print notthing at the end


def find_available_port(hostname: str, start: int, end: Optional[int] = None) -> int:
    if end is None:
        end = start + 100

    if hostname.startswith("http://"):
        hostname = hostname[7:]
    elif hostname.startswith("https://"):
        hostname = hostname[8:]
    else:
        raise Exception("Invalid hostname")

    for port in range(start, end):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex((hostname, port)) == 0:
                continue
            return port

    raise Exception(f"No available port between [{start}, {end})")


def wait_till_port_available(hostname: str, port: int, timeout: int = 10):
    """Wait for the port to be available. Return true if the port is available within the timeout. Otherwise false"""
    if hostname.startswith("http://"):
        hostname = hostname[7:]
    elif hostname.startswith("https://"):
        hostname = hostname[8:]
    else:
        raise Exception("Invalid hostname")

    poll_time = 0.1
    for i in range(timeout * int(1 / poll_time)):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex((hostname, port)) == 0:
                time.sleep(poll_time)
            else:
                return True
    return False


def is_port_available(hostname: str, port: int):
    if hostname.startswith("http://"):
        hostname = hostname[7:]
    elif hostname.startswith("https://"):
        hostname = hostname[8:]
    else:
        raise Exception("Invalid hostname")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return not (s.connect_ex((hostname, port)) == 0)
