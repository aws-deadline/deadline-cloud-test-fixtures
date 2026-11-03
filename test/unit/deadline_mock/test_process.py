# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import threading

import boto3
import pytest
from botocore.config import Config

from deadline_test_fixtures.deadline_mock import MockDeadlineScenario, MockDeadlineServerProcess
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


def test_process_server_exposes_resources_and_observability():
    process = MockDeadlineServerProcess()

    with process as server:
        assert server.base_url is not None
        assert server.backend is not None
        client = boto3.client(
            "deadline",
            endpoint_url=server.base_url,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-west-2",
            config=Config(inject_host_prefix=False),
        )

        client.list_farms()

        assert server.backend.call_counts == {"ListFarms": 1}
        assert server.backend.resources["farms"][0]["farmId"] == server.backend.farm_id

    assert process.base_url is None
    assert process.backend is None


def test_process_server_applies_scenario_options_and_exposes_created_jobs():
    # validate_job_templates=False accepting a template with no steps shows the option
    # crossed the process boundary; the job appearing in resources shows it is observable.
    scenario = MockDeadlineScenario.without_job_attachments(validate_job_templates=False)

    with MockDeadlineServerProcess(scenario) as server:
        assert server.base_url is not None
        assert server.backend is not None
        client = boto3.client(
            "deadline",
            endpoint_url=server.base_url,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-west-2",
            config=Config(inject_host_prefix=False),
        )

        queue = client.get_queue(farmId=server.backend.farm_id, queueId=server.backend.queue_id)
        job_id = client.create_job(
            farmId=server.backend.farm_id,
            queueId=server.backend.queue_id,
            priority=50,
            template="specificationVersion: jobtemplate-2023-09\nname: No Steps\n",
            templateType="YAML",
        )["jobId"]

        assert "jobAttachmentSettings" not in queue
        (job,) = server.backend.resources["jobs"]
        assert job["jobId"] == job_id
        assert job["name"] == "No Steps"
