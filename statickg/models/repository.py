from __future__ import annotations

import subprocess
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TypeAlias

from statickg.models.input_file import BaseType, InputFile

Pattern: TypeAlias = str


class Repository(ABC):
    @abstractmethod
    def glob(self, relpath: Pattern) -> list[InputFile]:
        raise NotImplementedError()

    @abstractmethod
    def fetch(self) -> bool:
        raise NotImplementedError()


class GitRepository(Repository):
    def __init__(self, repo: Path):
        self.repo = repo
        self.branch2files = {}
        self.current_commit = None

    def fetch(self) -> bool:
        """Fetch new data. Return True if there is new data"""
        # fetch from the remote repository
        output = subprocess.check_output(["git", "pull"], cwd=self.repo)
        if output != b"Already up to date.\n":
            self.current_commit = self.get_current_commit()
            return True

        current_commit_id = self.get_current_commit()
        if current_commit_id != self.current_commit:
            # user has manually updated the repository
            self.current_commit = current_commit_id
            return True

        return False

    def glob(self, relpath: Pattern) -> list[InputFile]:
        matched_files = {str(p.relative_to(self.repo)) for p in self.repo.glob(relpath)}
        return [file for file in self.all_files() if file.relpath in matched_files]

    def all_files(self, branch: str = "main") -> list[InputFile]:
        if branch not in self.branch2files:
            output = subprocess.check_output(
                ["git", "ls-tree", "-r", branch], cwd=self.repo
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
            self.branch2files[branch] = files

        return self.branch2files[branch]

    def get_current_commit(self):
        return (
            subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.repo)
            .decode()
            .strip()
        )
