# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import json
from contextlib import closing
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import boto3
import pytest
from botocore.config import Config

from deadline_test_fixtures.deadline_mock import (
    MockDeadlineBackend,
    MockDeadlineScenario,
    start_server,
)


def _client(endpoint_url: str):
    return boto3.client(
        "deadline",
        endpoint_url=endpoint_url,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name="us-west-2",
        config=Config(inject_host_prefix=False),
    )


@pytest.fixture
def mock_server():
    scenario = MockDeadlineScenario(response_delay_s=0)
    backend = MockDeadlineBackend(scenario)
    server, endpoint_url, _ = start_server(backend)
    try:
        yield backend, endpoint_url
    finally:
        server.shutdown()
        server.server_close()


def test_http_server_serves_submitter_resources_and_records_calls(mock_server):
    backend, endpoint_url = mock_server
    client = _client(endpoint_url)

    farms = client.list_farms()
    queues = client.list_queues(farmId=backend.farm_id)
    environments = client.list_queue_environments(
        farmId=backend.farm_id,
        queueId=backend.queue_id,
    )
    queue_role = client.assume_queue_role_for_user(
        farmId=backend.farm_id,
        queueId=backend.queue_id,
    )

    assert farms["farms"][0]["farmId"] == backend.farm_id
    assert queues["queues"][0]["queueId"] == backend.queue_id
    assert environments["environments"] == []
    assert queue_role["credentials"]["accessKeyId"] == "testing"
    assert backend.call_counts == {
        "AssumeQueueRoleForUser": 1,
        "ListFarms": 1,
        "ListQueues": 1,
        "ListQueueEnvironments": 1,
    }
    assert backend.unmatched_requests == []


def test_http_server_records_unmatched_requests(mock_server):
    backend, endpoint_url = mock_server

    with (
        pytest.raises(HTTPError) as error,
        closing(urlopen(f"{endpoint_url}/not-a-deadline-route")),
    ):
        pass

    assert error.value.code == 404
    assert backend.unmatched_requests == [("GET", "/not-a-deadline-route")]


def test_http_server_returns_validation_errors_for_malformed_requests(mock_server):
    _, endpoint_url = mock_server
    requests: list[str | Request] = [
        f"{endpoint_url}/2023-10-12/farms?maxResults=invalid",
        Request(
            f"{endpoint_url}/2023-10-12/farms",
            data=b"{",
            method="GET",
        ),
    ]

    for request in requests:
        with pytest.raises(HTTPError) as caught:
            urlopen(request, timeout=2)

        with closing(caught.value) as error:
            assert error.code == 400
            assert error.headers["x-amzn-errortype"] == "ValidationException"
            assert json.loads(error.read())["message"].startswith("Invalid request:")


def test_reset_restores_scenario_and_observability():
    backend = MockDeadlineBackend()
    backend.call_counts["ListFarms"] = 3
    backend.farms.clear()

    backend.reset()

    assert backend.call_counts == {}
    assert list(backend.farms) == [backend.farm_id]
    assert backend.snapshot()["identifiers"]["queue_id"] == backend.queue_id
