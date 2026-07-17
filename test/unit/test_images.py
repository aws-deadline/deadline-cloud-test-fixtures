# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from pathlib import Path

import numpy as np
import pytest
from PIL import Image

from deadline_test_fixtures.images import assert_images_close


def _write_image(path: Path, value: int, shape: tuple[int, int, int] = (2, 2, 3)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(np.full(shape, value, dtype=np.uint8)).save(path)


def test_assert_images_close_accepts_pixels_within_absolute_tolerance(tmp_path):
    _write_image(tmp_path / "expected" / "render.png", 10)
    _write_image(tmp_path / "actual" / "render.png", 12)

    assert_images_close(tmp_path / "expected", tmp_path / "actual")


def test_assert_images_close_reports_dimension_difference(tmp_path):
    _write_image(tmp_path / "expected" / "render.png", 10)
    _write_image(tmp_path / "actual" / "render.png", 10, shape=(3, 2, 3))

    with pytest.raises(AssertionError, match="dimensions differ"):
        assert_images_close(tmp_path / "expected", tmp_path / "actual")


def test_assert_images_close_can_match_collapsed_underscores(tmp_path):
    _write_image(tmp_path / "expected" / "render___1.png", 10)
    _write_image(tmp_path / "actual" / "render_1.png", 10)

    assert_images_close(
        tmp_path / "expected",
        tmp_path / "actual",
        collapse_underscores=True,
    )


def test_assert_images_close_can_allow_extra_files(tmp_path):
    _write_image(tmp_path / "expected" / "render.png", 10)
    _write_image(tmp_path / "actual" / "render.png", 10)
    _write_image(tmp_path / "actual" / "extra.png", 10)

    assert_images_close(
        tmp_path / "expected",
        tmp_path / "actual",
        allow_extra=True,
    )
