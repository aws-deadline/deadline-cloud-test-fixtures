# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Run the mock Deadline service in an independent Python process."""

from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import threading
import time
import urllib.request
from typing import Any

from .backend import ADMIN_RESET_PATH, ADMIN_STATE_PATH
from .scenario import MockDeadlineScenario

_SCENARIO_ENV = "DEADLINE_TEST_FIXTURES_MOCK_SCENARIO"


class RemoteDeadlineBackend:
    """Read-only view of backend state exposed by the admin endpoint."""

    def __init__(self, base_url: str, identifiers: dict[str, str]) -> None:
        self.base_url = base_url
        self._identifiers = identifiers

    @property
    def farm_id(self) -> str:
        return self._identifiers["farm_id"]

    @property
    def queue_id(self) -> str:
        return self._identifiers["queue_id"]

    def snapshot(self) -> dict[str, Any]:
        with urllib.request.urlopen(f"{self.base_url}{ADMIN_STATE_PATH}", timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))

    def reset(self) -> None:
        request = urllib.request.Request(
            f"{self.base_url}{ADMIN_RESET_PATH}", data=b"", method="POST"
        )
        with urllib.request.urlopen(request, timeout=5):
            pass

    @property
    def call_counts(self) -> dict[str, int]:
        return self.snapshot()["call_counts"]

    @property
    def request_log(self) -> list[tuple[str, str, str]]:
        return [tuple(request) for request in self.snapshot()["request_log"]]

    @property
    def unmatched_requests(self) -> list[tuple[str, str]]:
        return [tuple(request) for request in self.snapshot()["unmatched_requests"]]

    @property
    def resources(self) -> dict[str, list[dict[str, Any]]]:
        return self.snapshot()["resources"]


class MockDeadlineServerProcess:
    """Lifecycle handle for an out-of-process mock server.

    A separate process is required when native accessibility calls hold the
    test process's GIL while a DCC subprocess is making service requests.
    """

    def __init__(
        self,
        scenario: MockDeadlineScenario | None = None,
        *,
        startup_timeout: float = 60.0,
    ) -> None:
        self.scenario = scenario or MockDeadlineScenario()
        self.startup_timeout = startup_timeout
        self._process: subprocess.Popen[str] | None = None
        self.base_url: str | None = None
        self.backend: RemoteDeadlineBackend | None = None

    def start(self) -> MockDeadlineServerProcess:
        if self._process is not None:
            raise RuntimeError("Mock Deadline server process is already started")
        startup_deadline = time.monotonic() + self.startup_timeout
        environment = {
            **os.environ,
            _SCENARIO_ENV: json.dumps(self.scenario.to_dict()),
        }
        self._process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "deadline_test_fixtures.deadline_mock._server_child",
            ],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=None,
            stdin=subprocess.DEVNULL,
            text=True,
        )
        assert self._process.stdout is not None
        stdout = self._process.stdout
        startup_output: queue.Queue[str] = queue.Queue(maxsize=1)
        threading.Thread(
            target=lambda: startup_output.put(stdout.readline()),
            daemon=True,
        ).start()
        try:
            line = startup_output.get(timeout=self.startup_timeout).strip()
        except queue.Empty:
            self.stop()
            raise RuntimeError(
                f"Mock Deadline server did not produce a URL within " f"{self.startup_timeout}s"
            ) from None
        if not line.startswith("http"):
            return_code = self._process.poll()
            self.stop()
            raise RuntimeError(
                f"Mock Deadline server failed to start "
                f"(output={line!r}, returncode={return_code})"
            )
        self.base_url = line
        self.backend = RemoteDeadlineBackend(
            line,
            {
                "farm_id": self.scenario.farm_id,
                "queue_id": self.scenario.queue_id,
            },
        )
        self._wait_ready(startup_deadline)
        return self

    def _wait_ready(self, startup_deadline: float) -> None:
        assert self.backend is not None
        last_error: Exception | None = None
        while time.monotonic() < startup_deadline:
            try:
                self.backend.snapshot()
                return
            except Exception as error:
                last_error = error
                time.sleep(0.1)
        self.stop()
        raise RuntimeError(f"Mock Deadline server did not become ready: {last_error!r}")

    def stop(self) -> None:
        if self._process is not None and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)
        self._process = None
        self.base_url = None
        self.backend = None

    # PYI034 wants `Self` here, which needs typing_extensions on Python 3.9. This
    # class is concrete and not subclassed, so the concrete return type is accurate.
    def __enter__(self) -> MockDeadlineServerProcess:  # noqa: PYI034
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()
