# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import logging as _logging
import os as _os
from typing import Any as _Any, Optional as _Optional

import boto3 as _boto3
import pytest as _pytest

from .job_attachment_manager import JA_TEST_QUEUE_NAME, JA_TEST_QUEUE_NO_SETTINGS_NAME

_LOG = _logging.getLogger(__name__)

_root_logger = _logging.getLogger()
_log_filters: dict[str, _logging.Filter] = {}


class _PytestIdLoggerFilter(_logging.Filter):
    """Filter that prepends pytest IDs to logs"""

    def __init__(self, test_id: str) -> None:
        self.test_id = test_id

    def filter(self, record) -> bool:
        record.msg = f"[{self.test_id}] {record.msg}"
        return True


def _cleanup_orphaned_queues(deadline_client: _Any, farm_id: str) -> None:
    """
    Clean up orphaned queues that match known test resource patterns.

    This function is best-effort - it logs errors but doesn't raise exceptions
    to avoid masking actual test failures.
    """
    orphan_queue_names = {JA_TEST_QUEUE_NAME, JA_TEST_QUEUE_NO_SETTINGS_NAME}

    try:
        paginator = deadline_client.get_paginator("list_queues")
        for page in paginator.paginate(farmId=farm_id):
            for queue in page.get("queues", []):
                queue_name = queue.get("displayName", "")
                queue_id = queue.get("queueId", "")

                if queue_name in orphan_queue_names:
                    _LOG.warning(
                        f"Found orphaned test queue: {queue_name} ({queue_id}) - attempting cleanup"
                    )
                    try:
                        deadline_client.delete_queue(farmId=farm_id, queueId=queue_id)
                        _LOG.info(f"Successfully deleted orphaned queue: {queue_name} ({queue_id})")
                    except Exception as e:
                        _LOG.error(
                            f"Failed to delete orphaned queue {queue_name} ({queue_id}): {e}"
                        )
    except Exception as e:
        _LOG.error(f"Failed to list queues for orphan cleanup: {e}")


def pytest_sessionstart(session: _pytest.Session):
    # Base logging configuration
    formatter = _logging.Formatter("[%(asctime)s] %(message)s")
    for handler in _root_logger.handlers:
        handler.setFormatter(formatter)


def pytest_sessionfinish(session: _pytest.Session, exitstatus: int) -> None:
    """
    Pytest hook that runs at the end of the test session.

    This hook cleans up orphaned test resources (like queues) that may have been
    left behind due to test crashes or failures. It runs regardless of test outcome.
    """
    farm_id = _os.environ.get("FARM_ID")
    if not farm_id:
        _LOG.debug("FARM_ID not set, skipping orphan cleanup")
        return

    _LOG.info("Running orphaned resource cleanup...")

    try:
        deadline_client = _boto3.client("deadline")
        _cleanup_orphaned_queues(deadline_client, farm_id)
    except Exception as e:
        # Best-effort cleanup - don't fail the session if cleanup fails
        _LOG.error(f"Orphan cleanup failed: {e}")

    _LOG.info("Orphaned resource cleanup complete")


def pytest_runtest_logstart(nodeid: str, location: tuple[str, _Optional[int], str]):
    # Apply test ID log filter
    log_filter = _PytestIdLoggerFilter(nodeid)
    for handler in _root_logger.handlers:
        handler.addFilter(log_filter)
    _log_filters[nodeid] = log_filter


@_pytest.hookimpl(wrapper=True)
def pytest_runtest_teardown(item: _pytest.Item, nextitem: _Optional[_pytest.Item]):
    # Remove test ID log filter
    log_filter = _log_filters.pop(item.nodeid, None)
    if log_filter:
        for handler in _root_logger.handlers:
            handler.removeFilter(log_filter)

    yield
