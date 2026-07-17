# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import threading

import pytest

from deadline_test_fixtures.deadline_mock import MockDeadlineServerProcess
from deadline_test_fixtures.deadline_mock import process as process_module


def test_process_server_startup_timeout_covers_initial_output(monkeypatch):
    never_ready = threading.Event()

    class BlockingOutput:
        def readline(self):
            never_ready.wait()
            return ""

    class BlockingProcess:
        stdout = BlockingOutput()
        terminated = False

        def poll(self):
            return None

        def terminate(self):
            self.terminated = True

        def wait(self, timeout):
            return 0

        def kill(self):
            self.terminated = True

    process = BlockingProcess()
    monkeypatch.setattr(process_module.subprocess, "Popen", lambda *args, **kwargs: process)

    with pytest.raises(RuntimeError, match="did not produce a URL"):
        MockDeadlineServerProcess(startup_timeout=0.01).start()

    assert process.terminated
    never_ready.set()
