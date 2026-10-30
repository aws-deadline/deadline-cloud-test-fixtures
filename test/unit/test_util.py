# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from deadline_test_fixtures import util as mod


def _client_error(code: str, reason: str | None = None) -> ClientError:
    response: dict = {"Error": {"Code": code, "Message": "Request failed"}}
    if reason is not None:
        response["reason"] = reason
    return ClientError(response, "UpdateWorker")  # type: ignore[arg-type]


def _flaky(failures: int) -> MagicMock:
    func = MagicMock(side_effect=[RuntimeError("transient")] * failures + ["ok"])
    func.__name__ = "flaky"
    return func


def test_retry_with_predicate_jitter_adds_random_extra_delay() -> None:
    # GIVEN
    func = _flaky(2)
    decorated = mod.retry_with_predicate(max_attempts=3, delay=10, backoff=2, jitter=0.5)(func)
    with (
        patch.object(mod, "sleep") as mock_sleep,
        patch.object(mod.random, "uniform", side_effect=[1.5, 4.0]) as mock_uniform,
    ):
        # WHEN
        result = decorated()

    # THEN
    assert result == "ok"
    assert mock_uniform.call_args_list[0].args == (0, 5.0)
    assert mock_uniform.call_args_list[1].args == (0, 10.0)
    assert [c.args[0] for c in mock_sleep.call_args_list] == [11.5, 24.0]


def test_retry_with_predicate_without_jitter_sleeps_exact_delay() -> None:
    # GIVEN
    func = _flaky(2)
    decorated = mod.retry_with_predicate(max_attempts=3, delay=10, backoff=2)(func)
    with (
        patch.object(mod, "sleep") as mock_sleep,
        patch.object(mod.random, "uniform") as mock_uniform,
    ):
        # WHEN
        result = decorated()

    # THEN
    assert result == "ok"
    mock_uniform.assert_not_called()
    assert [c.args[0] for c in mock_sleep.call_args_list] == [10, 20]


@pytest.mark.parametrize(
    "error, expected",
    [
        pytest.param(
            _client_error("ConflictException", "CONCURRENT_MODIFICATION"), True, id="match"
        ),
        pytest.param(
            _client_error("ConflictException", "STATUS_CONFLICT"), False, id="wrong-reason"
        ),
        pytest.param(_client_error("ConflictException"), False, id="no-reason"),
        pytest.param(
            _client_error("ThrottlingException", "CONCURRENT_MODIFICATION"), False, id="wrong-code"
        ),
        pytest.param(RuntimeError("CONCURRENT_MODIFICATION"), False, id="not-client-error"),
    ],
)
def test_is_concurrent_modification(error: Exception, expected: bool) -> None:
    # WHEN / THEN
    assert mod.is_concurrent_modification(error) is expected


@pytest.mark.parametrize(
    "error, expected",
    [
        pytest.param(_client_error("ConflictException", "STATUS_CONFLICT"), True, id="match"),
        pytest.param(
            _client_error("ConflictException", "CONCURRENT_MODIFICATION"), False, id="wrong-reason"
        ),
        pytest.param(_client_error("ConflictException"), False, id="no-reason"),
        pytest.param(
            _client_error("ValidationException", "STATUS_CONFLICT"), False, id="wrong-code"
        ),
        pytest.param(RuntimeError("STATUS_CONFLICT"), False, id="not-client-error"),
    ],
)
def test_is_worker_status_conflict(error: Exception, expected: bool) -> None:
    # WHEN / THEN
    assert mod.is_worker_status_conflict(error) is expected
