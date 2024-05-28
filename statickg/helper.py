from __future__ import annotations

import importlib
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional, Type

import orjson
from hugedict.sqlite import SqliteDict
from loguru import logger

from statickg.models.etl import Change, ETLFileTracker
from statickg.models.input_file import InputFile, ProcessStatus, RelPath

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


def json_ser(obj: dict, indent: int = 0) -> bytes:
    if indent == 0:
        option = orjson.OPT_PASSTHROUGH_DATACLASS
    else:
        option = orjson.OPT_INDENT_2 | orjson.OPT_PASSTHROUGH_DATACLASS
    return orjson.dumps(obj, default=json_ser_default_object, option=option)


def json_ser_default_object(obj: Any):
    if isinstance(obj, RelPath):
        return obj.get_str()
    raise TypeError


def remove_deleted_files(
    newfiles: list[InputFile], outdir: Path, tracker: ETLFileTracker
):
    new_filenames = {file.path.stem for file in newfiles}
    for file in outdir.iterdir():
        if file.is_file() and file.stem not in new_filenames:
            tracker.file_changes.append((str(file), Change.REMOVE))
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
