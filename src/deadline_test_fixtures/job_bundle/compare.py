# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Structural comparison helpers for OpenJD job bundles."""

from __future__ import annotations

import copy
import json
import re
import subprocess
from dataclasses import dataclass, field
from difflib import unified_diff
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml

DEFAULT_BUNDLE_FILES = (
    "template.yaml",
    "parameter_values.yaml",
    "asset_references.yaml",
)


@dataclass(frozen=True)
class BundleNormalization:
    """Policy for removing expected machine/build differences."""

    replacements: Mapping[str, str] = field(default_factory=dict)
    regex_replacements: Sequence[tuple[str, str]] = ()
    normalized_parameter_values: Mapping[str, Any] = field(default_factory=dict)
    ignored_template_keys: Sequence[str] = ()
    normalize_path_separators: bool = True
    files: Sequence[str] = DEFAULT_BUNDLE_FILES


def _normalize_string(value: str, policy: BundleNormalization) -> str:
    for old, new in policy.replacements.items():
        value = value.replace(old, new)
    for pattern, replacement in policy.regex_replacements:
        value = re.sub(pattern, replacement, value)
    if policy.normalize_path_separators:
        value = value.replace("\\", "/")
    return value


def _normalize_value(value: Any, policy: BundleNormalization) -> Any:
    if isinstance(value, str):
        return _normalize_string(value, policy)
    if isinstance(value, list):
        return [_normalize_value(child, policy) for child in value]
    if isinstance(value, dict):
        return {key: _normalize_value(child, policy) for key, child in value.items()}
    return value


def _load_normalized(
    path: Path,
    policy: BundleNormalization,
) -> Any:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if path.name == "template.yaml" and isinstance(value, dict):
        value = copy.deepcopy(value)
        for key in policy.ignored_template_keys:
            value.pop(key, None)
    if path.name == "parameter_values.yaml" and isinstance(value, dict):
        value = copy.deepcopy(value)
        for parameter in value.get("parameterValues", []):
            name = parameter.get("name")
            if name in policy.normalized_parameter_values:
                parameter["value"] = policy.normalized_parameter_values[name]
    return _normalize_value(value, policy)


def _document_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False)


def assert_job_bundles_equal(
    expected_dir: Path,
    actual_dir: Path,
    *,
    normalization: BundleNormalization = BundleNormalization(),
) -> None:
    """Assert that required bundle documents are structurally equal."""
    missing_expected = [name for name in normalization.files if not (expected_dir / name).is_file()]
    missing_actual = [name for name in normalization.files if not (actual_dir / name).is_file()]
    if missing_expected or missing_actual:
        raise AssertionError(
            f"Missing job-bundle files: expected={missing_expected}, actual={missing_actual}"
        )

    differences: list[str] = []
    for name in normalization.files:
        expected = _load_normalized(expected_dir / name, normalization)
        actual = _load_normalized(actual_dir / name, normalization)
        if expected == actual:
            continue
        expected_text = _document_text(expected)
        actual_text = _document_text(actual)
        differences.append(
            "\n".join(
                unified_diff(
                    expected_text.splitlines(),
                    actual_text.splitlines(),
                    fromfile=f"expected/{name}",
                    tofile=f"actual/{name}",
                    lineterm="",
                )
            )
        )
    if differences:
        raise AssertionError("Job bundles differ:\n" + "\n".join(differences))


def find_complete_job_bundle(
    history_dir: Path,
    *,
    files: Sequence[str] = DEFAULT_BUNDLE_FILES,
) -> Path | None:
    """Return the newest complete ``history/YYYY-mm/bundle`` directory."""
    candidates = [path for path in history_dir.glob("*/*") if path.is_dir()]
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    required = set(files)
    for candidate in candidates:
        present = {path.name for path in candidate.iterdir() if path.is_file()}
        if required.issubset(present):
            return candidate
    return None


def assert_valid_job_bundle(template_path: Path) -> None:
    """Run ``openjd check`` and assert that validation succeeds."""
    result = subprocess.run(
        ["openjd", "check", str(template_path), "--output", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(f"openjd check failed with {result.returncode}:\n{result.stderr}")
    response = json.loads(result.stdout)
    if response.get("status") != "success":
        raise AssertionError(f"openjd check returned {response!r}")
