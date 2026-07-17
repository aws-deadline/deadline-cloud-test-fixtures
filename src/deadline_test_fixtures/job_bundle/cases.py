# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Conventional filesystem layout for DCC job-bundle cases."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree


@dataclass(frozen=True)
class JobBundleCase:
    """A test case with a runtime ``actual/`` output folder."""

    root: Path

    @property
    def actual_dir(self) -> Path:
        return self.root / "actual"

    def prepare_actual_dir(self) -> Path:
        rmtree(self.actual_dir, ignore_errors=True)
        self.actual_dir.mkdir(parents=True)
        return self.actual_dir
