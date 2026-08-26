# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from deadline_test_fixtures.deadline_mock import (
    build_mock_environment,
    write_deadline_config,
)


def test_write_deadline_config_selects_mock_resources(tmp_path):
    config_path = tmp_path / "deadline.config"
    history = tmp_path / "history"
    bundle_directory = tmp_path / "bundles"

    write_deadline_config(
        config_path,
        farm_id="farm-test",
        queue_id="queue-test",
        job_history_dir=history,
        job_bundle_default_directory=bundle_directory,
    )

    content = config_path.read_text(encoding="utf-8")
    assert "farm_id = farm-test" in content
    assert "queue_id = queue-test" in content
    assert f"job_history_dir = {history}" in content
    assert f"job_bundle_default_directory = {bundle_directory}" in content
    assert "submitter_update_notification = false" in content
    assert "opt_out = true" in content


def test_build_mock_environment_is_hermetic(tmp_path):
    environment = build_mock_environment(
        {"PATH": "existing"},
        deadline_endpoint_url="http://127.0.0.1:1234",
        config_path=tmp_path / "deadline.config",
        home_dir=tmp_path / "home",
    )

    assert environment["PATH"] == "existing"
    assert environment["AWS_ENDPOINT_URL_DEADLINE"] == "http://127.0.0.1:1234"
    assert environment["AWS_ACCESS_KEY_ID"] == "testing"
    assert environment["HOME"] == str(tmp_path / "home")
    assert environment["USERPROFILE"] == str(tmp_path / "home")
    assert environment["DEADLINE_CLOUD_MOCK_MODE"] == "1"
