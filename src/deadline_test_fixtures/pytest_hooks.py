# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import logging as _logging

import pytest as _pytest

_root_logger = _logging.getLogger()
_log_filters: dict[str, _logging.Filter] = {}


class _PytestIdLoggerFilter(_logging.Filter):
    """Filter that prepends pytest IDs to logs"""

    def __init__(self, test_id: str) -> None:
        self.test_id = test_id

    def filter(self, record) -> bool:
        record.msg = f"[{self.test_id}] {record.msg}"
        return True


def pytest_sessionstart(session: _pytest.Session):
    # Base logging configuration
    formatter = _logging.Formatter("[%(asctime)s] %(message)s")
    for handler in _root_logger.handlers:
        handler.setFormatter(formatter)


def pytest_runtest_logstart(nodeid: str, location: tuple[str, int | None, str]):
    # Apply test ID log filter
    log_filter = _PytestIdLoggerFilter(nodeid)
    for handler in _root_logger.handlers:
        handler.addFilter(log_filter)
    _log_filters[nodeid] = log_filter


@_pytest.hookimpl(wrapper=True)
def pytest_runtest_teardown(item: _pytest.Item, nextitem: _pytest.Item | None):
    # Remove test ID log filter
    log_filter = _log_filters.pop(item.nodeid, None)
    if log_filter:
        for handler in _root_logger.handlers:
            handler.removeFilter(log_filter)

    yield
