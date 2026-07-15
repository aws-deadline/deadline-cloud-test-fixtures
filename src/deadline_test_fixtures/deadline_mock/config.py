# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Configuration helpers for subprocesses that use the mock service."""

from __future__ import annotations

from pathlib import Path
from typing import Mapping, Optional

MOCK_ACCESS_KEY = "testing"
MOCK_SECRET_KEY = "testing"
MOCK_SESSION_TOKEN = "testing"
MOCK_REGION = "us-west-2"


def write_deadline_config(
    config_path: Path,
    *,
    farm_id: str,
    queue_id: str,
    job_history_dir: Path,
    profile_name: str = "(default)",
) -> None:
    """Write a minimal config selecting mock resources and a temporary history."""
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        f"[profile-{profile_name} defaults]\n"
        f"farm_id = {farm_id}\n"
        "\n"
        f"[profile-{profile_name} {farm_id} defaults]\n"
        f"queue_id = {queue_id}\n"
        "\n"
        f"[profile-{profile_name} settings]\n"
        f"job_history_dir = {job_history_dir}\n"
        "\n"
        "[settings]\n"
        "submitter_update_notification = false\n"
        "\n"
        "[telemetry]\n"
        "opt_out = true\n",
        encoding="utf-8",
    )


def build_mock_environment(
    base_env: Mapping[str, str],
    *,
    deadline_endpoint_url: str,
    config_path: Path,
    home_dir: Path,
    mock_mode_variable: Optional[str] = "DEADLINE_CLOUD_MOCK_MODE",
) -> dict[str, str]:
    """Create a hermetic environment for a Deadline client subprocess.

    ``mock_mode_variable`` is a consumer hook. DCC sidecars can use it to
    disable host-prefix injection or redirect ``management.*`` to loopback.
    Pass ``None`` when the subprocess does not need such a hook.
    """
    home_dir.mkdir(parents=True, exist_ok=True)
    environment = {
        **base_env,
        "AWS_ENDPOINT_URL_DEADLINE": deadline_endpoint_url,
        "AWS_ACCESS_KEY_ID": MOCK_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MOCK_SECRET_KEY,
        "AWS_SESSION_TOKEN": MOCK_SESSION_TOKEN,
        "AWS_DEFAULT_REGION": MOCK_REGION,
        "AWS_REGION": MOCK_REGION,
        "DEADLINE_CONFIG_FILE_PATH": str(config_path),
        "DEADLINE_CLOUD_TELEMETRY_OPT_OUT": "true",
        "HOME": str(home_dir),
        "USERPROFILE": str(home_dir),
    }
    if mock_mode_variable:
        environment[mock_mode_variable] = "1"
    return environment
