# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Image assertions shared by DCC render integration tests."""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
from PIL import Image


def _normalized_name(name: str) -> str:
    return re.sub(r"_+", "_", name)


def _match_actual(
    actual_dir: Path,
    expected_name: str,
    *,
    collapse_underscores: bool,
) -> Path:
    exact = actual_dir / expected_name
    if exact.is_file() or not collapse_underscores:
        return exact
    normalized = _normalized_name(expected_name)
    matches = [
        path
        for path in actual_dir.iterdir()
        if path.is_file() and _normalized_name(path.name) == normalized
    ]
    if len(matches) == 1:
        return matches[0]
    return exact


def assert_images_close(
    expected_dir: Path,
    actual_dir: Path,
    *,
    absolute_tolerance: float = 2,
    collapse_underscores: bool = False,
    allow_extra: bool = False,
) -> None:
    """Compare render files by shape and per-channel absolute tolerance."""
    expected_files = sorted(path for path in expected_dir.iterdir() if path.is_file())
    if not expected_files:
        raise AssertionError(f"No expected images found in {expected_dir}")

    matched_actual: set[Path] = set()
    for expected_path in expected_files:
        actual_path = _match_actual(
            actual_dir,
            expected_path.name,
            collapse_underscores=collapse_underscores,
        )
        if not actual_path.is_file():
            raise AssertionError(f"Missing rendered image: {actual_path}")
        matched_actual.add(actual_path)
        with Image.open(expected_path) as expected_image:
            expected = np.asarray(expected_image)
        with Image.open(actual_path) as actual_image:
            actual = np.asarray(actual_image)
        if actual.shape != expected.shape:
            raise AssertionError(
                f"Image dimensions differ for {expected_path.name}: "
                f"{actual.shape} != {expected.shape}"
            )
        if not np.allclose(actual, expected, atol=absolute_tolerance):
            maximum_delta = float(
                np.abs(actual.astype(np.float64) - expected.astype(np.float64)).max()
            )
            raise AssertionError(
                f"Image pixels differ for {expected_path.name}: "
                f"maximum delta {maximum_delta} exceeds {absolute_tolerance}"
            )

    unexpected = sorted(
        path.name for path in actual_dir.iterdir() if path.is_file() and path not in matched_actual
    )
    if unexpected and not allow_extra:
        raise AssertionError(f"Unexpected rendered images in {actual_dir}: {unexpected}")
