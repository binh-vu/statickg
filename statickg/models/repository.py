from __future__ import annotations

import subprocess
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional, TypeAlias

from statickg.models.input_file import BaseType, InputFile

Pattern: TypeAlias = str


class Repository(ABC):
    @abstractmethod
    def glob(self, relpath: Pattern) -> list[InputFile]:
        raise NotImplementedError()

    @abstractmethod
    def fetch(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def get_version_id(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def get_version_creation_time(self) -> datetime:
        raise NotImplementedError()


class GitRepository(Repository):
    def __init__(self, repo: Path):
        self.repo = repo
        self.commit2files = {}
        self.current_commit = None

    def fetch(self) -> bool:
        """Fetch new data. Return True if there is new data"""
        # fetch from the remote repository
        output = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=self.repo
        )
        if output.decode().strip() != "HEAD":
            # we are in a branch and we can fetch the latest changes -- otherwise, we are in a detached HEAD state
            # and we cannot fetch the latest changes (doing nothing)
            # we should rely on commit id instead of results of git pull
            output = subprocess.check_output(["git", "pull"], cwd=self.repo)

        current_commit_id = self.get_current_commit()
        if current_commit_id != self.current_commit:
            # user has manually updated the repository
            self.current_commit = current_commit_id
            return True

        return False

    def glob(self, relpath: Pattern) -> list[InputFile]:
        matched_files = {str(p.relative_to(self.repo)) for p in self.repo.glob(relpath)}
        return [file for file in self.all_files() if file.relpath in matched_files]

    def all_files(self, commit_id: Optional[str] = None) -> list[InputFile]:
        if commit_id is None:
            commit_id = self.get_current_commit()

        if commit_id not in self.commit2files:
            output = subprocess.check_output(
                ["git", "ls-tree", "-r", commit_id], cwd=self.repo
            )

            content = output.decode().strip().split("\n")
            files = []
            for line in content:
                objectmode, objecttype, objectname, relpath = line.split()
                assert objecttype == "blob"
                files.append(
                    InputFile(
                        basetype=BaseType.REPO,
                        relpath=relpath,
                        path=self.repo / relpath,
                        key=objectname,
                    )
                )
            self.commit2files[commit_id] = files

        return self.commit2files[commit_id]

    def get_current_commit(self):
        return (
            subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.repo)
            .decode()
            .strip()
        )

    def get_commit_time(self, commit_id: str):
        unix_timestamp = (
            subprocess.check_output(
                ["git", "show", "--no-patch", "--format=%ct", commit_id], cwd=self.repo
            )
            .decode()
            .strip()
        )
        return datetime.fromtimestamp(int(unix_timestamp))

    def get_version_id(self) -> str:
        return self.get_current_commit()

    def get_version_creation_time(self) -> datetime:
        return self.get_commit_time(self.get_current_commit())
