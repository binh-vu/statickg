from __future__ import annotations

import importlib
from typing import Callable


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
