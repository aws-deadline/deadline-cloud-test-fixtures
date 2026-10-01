# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Child-process entry point for :class:`MockDeadlineServerProcess`."""

from __future__ import annotations

import json
import os
import sys

from .backend import MockDeadlineBackend, start_server
from .process import _SCENARIO_ENV
from .scenario import MockDeadlineScenario


def main() -> None:
    serialized = os.environ.get(_SCENARIO_ENV)
    scenario = (
        MockDeadlineScenario.from_dict(json.loads(serialized))
        if serialized
        else MockDeadlineScenario()
    )
    backend = MockDeadlineBackend(scenario)
    backend.log_callback = lambda message: print(
        f"[mock-deadline] {message}", file=sys.stderr, flush=True
    )
    server, base_url, thread = start_server(backend)
    print(base_url, flush=True)
    try:
        thread.join()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
