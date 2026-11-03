# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import json
import socket
from contextlib import closing
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from deadline_test_fixtures.deadline_mock import (
    MockDeadlineBackend,
    MockDeadlineScenario,
    start_server,
)

_JOB_TEMPLATE = """\
specificationVersion: jobtemplate-2023-09
name: Render {{Param.Frames}}
parameterDefinitions:
- name: Frames
  type: STRING
  default: "1-2"
- name: SceneFile
  type: PATH
- name: Width
  type: INT
steps:
- name: Render
  script:
    actions:
      onRun:
        command: echo
"""

_CONDA_QUEUE_ENVIRONMENT = {
    "queueEnvironmentId": "queueenv-0000000000000000000000000000000c",
    "name": "Conda",
    "priority": 1,
    "templateType": "YAML",
    "template": """\
specificationVersion: environment-2023-09
parameterDefinitions:
- name: CondaPackages
  type: STRING
  default: ""
environment:
  name: Conda
  script:
    actions:
      onEnter:
        command: echo
""",
}

_VALID_PARAMETERS = {"SceneFile": {"path": "/scenes/shot.ma"}, "Width": {"int": "960"}}


def _serve(scenario: MockDeadlineScenario):
    backend = MockDeadlineBackend(scenario)
    server, endpoint_url, _ = start_server(backend)
    return backend, server, endpoint_url


def _create_job(client, backend, **overrides):
    request = {
        "farmId": backend.farm_id,
        "queueId": backend.queue_id,
        "priority": 50,
        "template": _JOB_TEMPLATE,
        "templateType": "YAML",
        "parameters": _VALID_PARAMETERS,
        **overrides,
    }
    return client.create_job(**{key: value for key, value in request.items() if value is not None})


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


def test_http_server_starts_and_serves_without_reverse_dns():
    backend = MockDeadlineBackend(MockDeadlineScenario(response_delay_s=0))
    with patch.object(socket, "getfqdn", side_effect=AssertionError("unexpected DNS lookup")):
        server, endpoint_url, _ = start_server(backend)
    try:
        assert server.server_address[0] == server.server_name == "127.0.0.1"
        assert server.server_port == server.server_address[1]
        assert endpoint_url == f"http://127.0.0.1:{server.server_port}"
        assert _client(endpoint_url).list_farms()["farms"][0]["farmId"] == backend.farm_id
        assert backend.call_counts == {"ListFarms": 1}
        assert backend.unmatched_requests == []
    finally:
        server.shutdown()
        server.server_close()


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
    backend.jobs[(backend.farm_id, backend.queue_id, "job-1")] = {"jobId": "job-1"}

    backend.reset()

    assert backend.call_counts == {}
    assert list(backend.farms) == [backend.farm_id]
    assert backend.jobs == {}
    assert backend.snapshot()["identifiers"]["queue_id"] == backend.queue_id


def test_create_job_records_the_submission_and_get_job_reports_it_created(mock_server):
    backend, endpoint_url = mock_server
    client = _client(endpoint_url)

    job_id = _create_job(client, backend, targetTaskRunStatus="SUSPENDED", maxRetriesPerTask=5)[
        "jobId"
    ]
    job = client.get_job(farmId=backend.farm_id, queueId=backend.queue_id, jobId=job_id)

    assert job["name"] == "Render 1-2"
    assert job["lifecycleStatus"] == "CREATE_COMPLETE"
    assert job["taskRunStatus"] == "SUSPENDED"
    assert job["priority"] == 50
    assert job["parameters"] == _VALID_PARAMETERS
    (stored,) = backend.snapshot()["resources"]["jobs"]
    assert stored["jobId"] == job_id
    assert stored["template"] == _JOB_TEMPLATE
    assert stored["templateType"] == "YAML"
    assert stored["maxRetriesPerTask"] == 5
    assert backend.call_counts == {"CreateJob": 1, "GetJob": 1}
    assert backend.unmatched_requests == []


def test_create_job_uses_name_override(mock_server):
    backend, endpoint_url = mock_server
    client = _client(endpoint_url)

    job_id = _create_job(client, backend, nameOverride="Override")["jobId"]

    job = client.get_job(farmId=backend.farm_id, queueId=backend.queue_id, jobId=job_id)
    assert job["name"] == "Override"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        pytest.param(
            {"template": "specificationVersion: jobtemplate-2023-09\nname: x\n"},
            "steps",
            id="invalid-template",
        ),
        pytest.param({"template": "- not a mapping"}, "key-value pairs", id="not-a-mapping"),
        pytest.param(
            {"parameters": {**_VALID_PARAMETERS, "CondaPackages": {"string": "maya"}}},
            "not defined in the template: CondaPackages",
            id="unknown-parameter",
        ),
        pytest.param(
            {"parameters": {"SceneFile": {"path": "/scenes/shot.ma"}}},
            "missing for required job parameters: Width",
            id="missing-required-parameter",
        ),
        pytest.param(
            {"parameters": {**_VALID_PARAMETERS, "Width": {"int": "wide"}}},
            "Width",
            id="invalid-int",
        ),
        pytest.param(
            {"template": None, "templateType": None, "parameters": None, "sourceJobId": "job-1"},
            "sourceJobId",
            id="copy-job",
        ),
    ],
)
def test_create_job_rejects_submissions_the_service_would(mock_server, overrides, message):
    backend, endpoint_url = mock_server
    client = _client(endpoint_url)

    with pytest.raises(ClientError) as caught:
        _create_job(client, backend, **overrides)

    assert caught.value.response["Error"]["Code"] == "ValidationException"
    assert message in caught.value.response["Error"]["Message"]
    assert backend.jobs == {}


def test_create_job_accepts_parameters_defined_by_queue_environments():
    backend, server, endpoint_url = _serve(
        MockDeadlineScenario(queue_environments=(_CONDA_QUEUE_ENVIRONMENT,))
    )
    try:
        client = _client(endpoint_url)

        job_id = _create_job(
            client,
            backend,
            parameters={**_VALID_PARAMETERS, "CondaPackages": {"string": "maya=2026.*"}},
        )["jobId"]

        job = client.get_job(farmId=backend.farm_id, queueId=backend.queue_id, jobId=job_id)
        assert job["parameters"]["CondaPackages"] == {"string": "maya=2026.*"}
    finally:
        server.shutdown()
        server.server_close()


def test_create_job_skips_validation_when_the_scenario_disables_it():
    backend, server, endpoint_url = _serve(MockDeadlineScenario(validate_job_templates=False))
    try:
        client = _client(endpoint_url)

        named = _create_job(
            client,
            backend,
            template="specificationVersion: jobtemplate-2023-09\nname: No Steps\n",
            parameters={"Anything": {"string": "goes"}},
        )["jobId"]
        unparseable = _create_job(client, backend, template="- not a mapping")["jobId"]

        def name(job_id):
            return client.get_job(farmId=backend.farm_id, queueId=backend.queue_id, jobId=job_id)[
                "name"
            ]

        assert name(named) == "No Steps"
        assert name(unparseable) == unparseable
    finally:
        server.shutdown()
        server.server_close()


def test_get_job_reports_unknown_jobs_as_not_found(mock_server):
    backend, endpoint_url = mock_server

    with pytest.raises(ClientError) as caught:
        _client(endpoint_url).get_job(
            farmId=backend.farm_id, queueId=backend.queue_id, jobId="job-missing"
        )

    assert caught.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_without_job_attachments_scenario_serves_a_queue_without_attachment_settings():
    scenario = MockDeadlineScenario.without_job_attachments(validate_job_templates=False)
    backend, server, endpoint_url = _serve(scenario)
    try:
        queue = _client(endpoint_url).get_queue(farmId=backend.farm_id, queueId=backend.queue_id)
    finally:
        server.shutdown()
        server.server_close()

    assert "jobAttachmentSettings" not in queue
    assert "jobAttachmentSettings" in MockDeadlineScenario().queue
    assert MockDeadlineScenario.from_dict(scenario.to_dict()) == scenario


@pytest.mark.parametrize(
    "value",
    [{"int": "960", "string": "960"}, {"bool": "true"}, "960"],
    ids=["two-types", "unknown-type", "not-a-union"],
)
def test_create_job_rejects_malformed_parameter_values(value):
    # boto3 validates the JobParameter union client-side, so call the backend directly.
    backend = MockDeadlineBackend()

    with pytest.raises(ClientError) as caught:
        backend.create_job(
            farmId=backend.farm_id,
            queueId=backend.queue_id,
            priority=50,
            template=_JOB_TEMPLATE,
            templateType="YAML",
            parameters={**_VALID_PARAMETERS, "Width": value},
        )

    assert caught.value.response["Error"]["Code"] == "ValidationException"
    assert "Width" in caught.value.response["Error"]["Message"]


def test_create_job_ignores_queue_environments_without_a_template():
    summary_only = {
        "queueEnvironmentId": "queueenv-0000000000000000000000000000000d",
        "name": "SummaryOnly",
        "priority": 2,
    }
    backend = MockDeadlineBackend(MockDeadlineScenario(queue_environments=(summary_only,)))

    result = backend.create_job(
        farmId=backend.farm_id,
        queueId=backend.queue_id,
        priority=50,
        template=_JOB_TEMPLATE,
        templateType="YAML",
        parameters=_VALID_PARAMETERS,
    )

    assert backend.jobs[(backend.farm_id, backend.queue_id, result["jobId"])]["name"] == (
        "Render 1-2"
    )
