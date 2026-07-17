# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Cross-platform accessibility application discovery."""

from __future__ import annotations

import time
from typing import Optional

import xa11y

STARTUP_TIMEOUT = 60.0


def find_accessibility_app(
    pid: int,
    timeout: float = STARTUP_TIMEOUT,
    *,
    name_prefix: Optional[str] = None,
) -> xa11y.App:
    """Find an accessibility app by PID and optional name prefix.

    ``name_prefix`` is useful for DCC-hosted Qt dialogs which appear as a
    separate accessibility app sharing the host process ID on Windows.
    """
    deadline = time.monotonic() + timeout
    last_apps: list[xa11y.App] = []
    while time.monotonic() < deadline:
        try:
            last_apps = xa11y.App.list()
            matching_pid = [app for app in last_apps if app.pid == pid]
            if name_prefix is not None:
                matching_pid = [
                    app for app in matching_pid if app.name and app.name.startswith(name_prefix)
                ]
            if matching_pid:
                return matching_pid[0]
        except Exception:
            # Accessibility backends can fail transiently while applications start.
            pass
        time.sleep(0.25)
    apps = [(app.name, app.pid) for app in last_apps]
    qualifier = f" and name prefix {name_prefix!r}" if name_prefix else ""
    raise TimeoutError(f"No accessibility app appeared for PID {pid}{qualifier}; apps={apps}")
