# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from pathlib import Path

import pytest
import yaml

from deadline_test_fixtures.job_bundle import (
    BundleNormalization,
    JobBundleCase,
    assert_job_bundles_equal,
    find_complete_job_bundle,
)


def _write_bundle(directory: Path, root: str = "ROOT") -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "template.yaml").write_text(
        yaml.safe_dump(
            {
                "specificationVersion": "jobtemplate-2023-09",
                "name": "Test",
                "jobEnvironments": [{"name": "moving"}],
                "steps": [],
            }
        ),
        encoding="utf-8",
    )
    (directory / "parameter_values.yaml").write_text(
        yaml.safe_dump(
            {
                "parameterValues": [
                    {"name": "SceneFile", "value": f"{root}\\scene.test"},
                    {"name": "SubmitterIntegrationVersion", "value": "1.2.3"},
                ]
            }
        ),
        encoding="utf-8",
    )
    (directory / "asset_references.yaml").write_text(
        yaml.safe_dump({"assetReferences": {"inputs": {"filenames": [f"{root}\\scene.test"]}}}),
        encoding="utf-8",
    )


def test_bundle_comparison_uses_structural_normalization(tmp_path):
    expected = tmp_path / "expected"
    actual = tmp_path / "actual"
    _write_bundle(expected, "PLACEHOLDER")
    _write_bundle(actual, "/workspace")

    assert_job_bundles_equal(
        expected,
        actual,
        normalization=BundleNormalization(
            replacements={"PLACEHOLDER": "/workspace"},
            normalized_parameter_values={"SubmitterIntegrationVersion": "NORMALIZED"},
        ),
    )


def test_bundle_comparison_reports_structural_diff(tmp_path):
    expected = tmp_path / "expected"
    actual = tmp_path / "actual"
    _write_bundle(expected)
    _write_bundle(actual)
    (actual / "template.yaml").write_text("name: Different\n", encoding="utf-8")

    with pytest.raises(AssertionError, match="Job bundles differ"):
        assert_job_bundles_equal(expected, actual)


def test_bundle_comparison_only_ignores_explicit_template_keys(tmp_path):
    expected = tmp_path / "expected"
    actual = tmp_path / "actual"
    _write_bundle(expected)
    _write_bundle(actual)
    actual_template = yaml.safe_load((actual / "template.yaml").read_text(encoding="utf-8"))
    actual_template["jobEnvironments"] = [{"name": "changed"}]
    (actual / "template.yaml").write_text(
        yaml.safe_dump(actual_template),
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="Job bundles differ"):
        assert_job_bundles_equal(expected, actual)

    assert_job_bundles_equal(
        expected,
        actual,
        normalization=BundleNormalization(ignored_template_keys=("jobEnvironments",)),
    )


def test_find_complete_bundle(tmp_path):
    history_bundle = tmp_path / "history" / "2026-07" / "bundle"
    _write_bundle(history_bundle)
    assert find_complete_job_bundle(tmp_path / "history") == history_bundle


def test_job_bundle_case_prepares_actual_directory(tmp_path):
    case = JobBundleCase(tmp_path / "cube")
    case.actual_dir.mkdir(parents=True)
    (case.actual_dir / "stale").write_text("old", encoding="utf-8")

    result = case.prepare_actual_dir()

    assert result == case.actual_dir
    assert list(result.iterdir()) == []
