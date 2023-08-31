# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import botocore.exceptions
import logging
from time import sleep
from typing import Any, Callable

LOG = logging.getLogger(__name__)


def wait_for(
    *,
    description: str,
    predicate: Callable[[], bool],
    interval_s: float,
    max_retries: int | None = None,
) -> None:
    if max_retries is not None:
        assert max_retries > 0, "max_retries must be a positive integer"
    assert interval_s > 0, "interval_s must be a positive number"

    LOG.info(f"Waiting for {description}")
    retry_count = 0
    while not predicate():
        if max_retries and retry_count >= max_retries:
            raise TimeoutError(f"Timed out waiting for {description}")

        LOG.info(f"Retrying in {interval_s}s...")
        retry_count += 1
        sleep(interval_s)


def call_api(*, description: str, fn: Callable[[], Any]) -> Any:
    LOG.info(f"About to call API ({description})")
    try:
        response = fn()
    except botocore.exceptions.ClientError as e:
        LOG.error(f"API call failed ({description})")
        LOG.exception(f"The following exception was raised: {e}")
        raise
    else:
        LOG.info(f"API call succeeded ({description})")
        return response


def clean_kwargs(kwargs: dict) -> dict:
    """Removes None from kwargs dicts"""
    return {k: v for k, v in kwargs.items() if v is not None}
