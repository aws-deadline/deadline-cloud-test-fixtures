# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import botocore.exceptions
import logging
from time import sleep
from typing import Any, Callable
import functools


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


def is_instance_not_ready(e: Exception):
    # Retry on the instance not being ready.
    return (
        isinstance(e, botocore.exceptions.ClientError)
        and e.response["Error"]["Code"] == "InvalidInstanceId"
    )


def retry_with_predicate(max_attempts=3, delay=1, backoff=2, predicate=None):
    """
    Retry decorator with configurable parameters and exception predicate.

    Args:
        max_attempts (int): Maximum number of retry attempts
        delay (float): Initial delay between retries in seconds
        backoff (float): Backoff multiplier (e.g. 2 means delay doubles each retry)
        predicate (callable): Function that takes an exception and returns True if retry should happen
                             If None, all exceptions will trigger a retry

    Returns:
        The decorated function result if successful
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            current_delay = delay

            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1

                    # Check if we should retry based on the exception
                    should_retry = predicate(e) if predicate else True

                    # If we shouldn't retry or we're out of attempts, raise the exception
                    if not should_retry or attempts >= max_attempts:
                        raise

                    # Log the retry attempt
                    logging.warning(
                        f"Retry {attempts}/{max_attempts} for {func.__name__} due to {e.__class__.__name__}: {str(e)}. "
                        f"Retrying in {current_delay} seconds..."
                    )

                    # Wait before retrying
                    sleep(current_delay)

                    # Increase the delay for the next attempt
                    current_delay *= backoff

        return wrapper

    return decorator


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
