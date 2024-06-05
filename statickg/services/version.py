from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Mapping, NotRequired, TypedDict

from tqdm import tqdm

from statickg.helper import get_latest_version, logger_helper
from statickg.models.input_file import ProcessStatus
from statickg.models.prelude import Change, ETLFileTracker, RelPath, Repository
from statickg.services.interface import BaseFileService, BaseService


class VersionServiceConstructArgs(TypedDict):
    pass


class VersionServiceInvokeArgs(TypedDict):
    pass


class VersionService(BaseFileService[VersionServiceInvokeArgs]):
    """A service that can generate version of knowledge graph"""

    pass
