# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import datetime
import json
import re
from collections.abc import Generator
from dataclasses import asdict, replace
from datetime import timedelta
from typing import Any, cast
from unittest.mock import MagicMock, call, patch

import pytest

from deadline_test_fixtures import (
    CloudWatchLogEvent,
    Farm,
    Fleet,
    Job,
    JobAttachmentSettings,
    Queue,
    QueueFleetAssociation,
    Session,
    Step,
    Task,
    TaskStatus,
)
from deadline_test_fixtures.deadline import resources as mod
from deadline_test_fixtures.models import JobRunAsUser, PosixSessionUser, WindowsSessionUser


@pytest.fixture(autouse=True)
def wait_for_shim() -> Generator[None, None, None]:
    import sys

    from deadline_test_fixtures.util import wait_for

    # Force the wait_for to have a short interval for unit tests
    def wait_for_shim(*args, **kwargs):
        kwargs.pop("interval_s", None)
        kwargs.pop("max_retries", None)
        wait_for(*args, **kwargs, interval_s=sys.float_info.epsilon, max_retries=None)

    with patch.object(mod, "wait_for", wait_for_shim):
        yield


@pytest.fixture
def farm() -> Farm:
    return Farm(id="farm-123")


@pytest.fixture
def queue(farm: Farm) -> Queue:
    return Queue(id="queue-123", farm=farm)


@pytest.fixture
def fleet_id() -> str:
    return "fleet-123"


@pytest.fixture
def fleet(
    farm: Farm,
    fleet_id: str,
) -> Fleet:
    return Fleet(id=fleet_id, farm=farm)


@pytest.fixture
def qfa(
    farm: Farm,
    queue: Queue,
    fleet: Fleet,
) -> QueueFleetAssociation:
    return QueueFleetAssociation(
        farm=farm,
        queue=queue,
        fleet=fleet,
    )


@pytest.fixture
def job_id() -> str:
    return "job-123"


@pytest.fixture
def job(
    farm: Farm,
    queue: Queue,
    job_id: str,
) -> Job:
    return Job(
        id=job_id,
        farm=farm,
        queue=queue,
        template={},
        name="Job Name",
        lifecycle_status=mod.JobLifecycleStatus.CREATE_COMPLETE,
        lifecycle_status_message="Nice",
        priority=1,
        created_at=datetime.datetime.now(),
        created_by="test-user",
    )


@pytest.fixture
def step_id() -> str:
    return "step-123"


@pytest.fixture
def step(
    farm: Farm,
    queue: Queue,
    job: Job,
    step_id: str,
) -> Step:
    return Step(
        id=step_id,
        farm=farm,
        queue=queue,
        job=job,
        name="Step Name",
        created_at=datetime.datetime.now(),
        created_by="test-user",
        lifecycle_status=mod.StepLifecycleStatus.CREATE_COMPLETE,
        task_run_status=TaskStatus.SUCCEEDED,
        task_run_status_counts={
            TaskStatus.ASSIGNED: 0,
            TaskStatus.CANCELED: 0,
            TaskStatus.FAILED: 0,
            TaskStatus.INTERRUPTING: 0,
            TaskStatus.NOT_COMPATIBLE: 0,
            TaskStatus.PENDING: 0,
            TaskStatus.READY: 0,
            TaskStatus.RUNNING: 0,
            TaskStatus.SCHEDULED: 0,
            TaskStatus.STARTING: 0,
            TaskStatus.SUCCEEDED: 0,
            TaskStatus.SUSPENDED: 0,
        },
    )


@pytest.fixture
def session_id_hex() -> str:
    return "00001111222233334444555566667777"


@pytest.fixture
def session_id(session_id_hex: str) -> str:
    return f"session-{session_id_hex}"


@pytest.fixture
def session_action_id(session_id_hex: str) -> str:
    return f"sessionaction-{session_id_hex}-0"


@pytest.fixture
def task_id() -> str:
    return "task-a12"


@pytest.fixture
def task_created_at() -> datetime.datetime:
    return datetime.datetime.now()


@pytest.fixture
def task_created_by() -> str:
    return "taskcreator"


@pytest.fixture
def task_run_status() -> TaskStatus:
    return TaskStatus.SUCCEEDED


@pytest.fixture
def task(
    farm: Farm,
    queue: Queue,
    job: Job,
    step: Step,
    session_action_id: str,
    task_id: str,
    task_created_at: datetime.datetime,
    task_created_by: str,
    task_run_status: TaskStatus,
) -> Task:
    return Task(
        farm=farm,
        queue=queue,
        job=job,
        step=step,
        id=task_id,
        created_at=task_created_at,
        created_by=task_created_by,
        run_status=task_run_status,
        latest_session_action_id=session_action_id,
    )


@pytest.fixture
def session_log_driver() -> str:
    return "awslogs"


@pytest.fixture
def session_log_group_name() -> str:
    return "sessionLogGroup"


@pytest.fixture
def session_log_stream_name() -> str:
    return "sessionLogStream"


@pytest.fixture
def session_log_config(
    session_log_driver: str,
    session_log_group_name: str,
    session_log_stream_name: str,
) -> mod.LogConfiguration:
    return mod.LogConfiguration(
        log_driver=session_log_driver,  # type: ignore[arg-type]
        options={
            "logGroupName": session_log_group_name,
            "logStreamName": session_log_stream_name,
        },
        parameters={},
    )


@pytest.fixture
def worker_log_driver() -> str:
    return "awslogs"


@pytest.fixture
def worker_log_group_name() -> str:
    return "workerLogGroup"


@pytest.fixture
def worker_log_stream_name() -> str:
    return "workerLogStream"


@pytest.fixture
def worker_log_config(
    worker_log_driver: str,
    worker_log_group_name: str,
    worker_log_stream_name: str,
) -> mod.LogConfiguration:
    return mod.LogConfiguration(
        log_driver=worker_log_driver,  # type: ignore[arg-type]
        options={
            "logGroupName": worker_log_group_name,
            "logStreamName": worker_log_stream_name,
        },
        parameters={},
    )


@pytest.fixture
def worker_id() -> str:
    return "worker-abc"


@pytest.fixture
def session_started_at() -> datetime.datetime:
    return datetime.datetime.now()


@pytest.fixture
def session_lifecycle_status() -> mod.SessionLifecycleStatus:
    return mod.SessionLifecycleStatus.ENDED


@pytest.fixture
def ip_v4_addresses() -> list[str]:
    return ["192.168.0.100"]


@pytest.fixture
def ip_v6_addresses() -> list[str]:
    return ["::1"]


@pytest.fixture
def ip_addresses(
    ip_v4_addresses: list[str],
    ip_v6_addresses: list[str],
) -> mod.IpAddresses:
    return mod.IpAddresses(
        ip_v4_addresses=ip_v4_addresses,
        ip_v6_addresses=ip_v6_addresses,
    )


@pytest.fixture
def ec2_instance_arn() -> str:
    return "ec2_instance_arn"


@pytest.fixture
def ec2_instance_type() -> str:
    return "t3.micro"


@pytest.fixture
def host_name() -> str:
    return "hostname"


@pytest.fixture
def worker_host_properties(
    ec2_instance_arn: str,
    ec2_instance_type: str,
    host_name: str,
    ip_addresses: mod.IpAddresses,
) -> mod.WorkerHostProperties:
    return mod.WorkerHostProperties(
        ec2_instance_arn=ec2_instance_arn,
        ec2_instance_type=ec2_instance_type,
        ip_addresses=ip_addresses,
        host_name=host_name,
    )


@pytest.fixture
def session(
    farm: Farm,
    queue: Queue,
    job: Job,
    fleet: Fleet,
    session_id: str,
    session_lifecycle_status: mod.SessionLifecycleStatus,
    session_log_config: mod.LogConfiguration,
    session_started_at: datetime.datetime,
    worker_id: str,
    worker_log_config: mod.LogConfiguration,
) -> Session:
    return Session(
        farm=farm,
        queue=queue,
        fleet=fleet,
        job=job,
        id=session_id,
        lifecycle_status=session_lifecycle_status,
        logs=session_log_config,
        started_at=session_started_at,
        worker_id=worker_id,
        worker_log=worker_log_config,
    )


@pytest.fixture(scope="function")
def log_client():
    def configure_log_client(session: Session, log_messages: list[str]):
        logs_client = MagicMock()
        logs = mod.SessionLog(
            session_id=session.id,
            logs=[
                mod.CloudWatchLogEvent(
                    ingestion_time=i,
                    message=message,
                    timestamp=i,
                )
                for i, message in enumerate(log_messages)
            ],
        )
        return logs_client, logs

    return configure_log_client


class TestFarm:
    def test_create(self) -> None:
        # GIVEN
        display_name = "test-farm"
        farm_id = "farm-123"
        mock_client = MagicMock()
        mock_client.create_farm.return_value = {"farmId": farm_id}

        # WHEN
        result = Farm.create(client=mock_client, display_name=display_name)

        # THEN
        assert result.id == farm_id
        mock_client.create_farm.assert_called_once_with(
            displayName=display_name,
        )

    def test_delete(self, farm: Farm) -> None:
        # GIVEN
        mock_client = MagicMock()

        # WHEN
        farm.delete(client=mock_client)

        # THEN
        mock_client.delete_farm.assert_called_once_with(farmId=farm.id)


class TestQueue:
    def test_create(self, farm: Farm) -> None:
        # GIVEN
        display_name = "test-queue"
        queue_id = "queue-123"
        role_arn = "arn:aws:iam::123456789123:role/TestRole"
        job_attachments = JobAttachmentSettings(bucket_name="bucket", root_prefix="root")
        mock_client = MagicMock()
        mock_client.create_queue.return_value = {"queueId": queue_id}
        job_run_as_user = JobRunAsUser(
            posix=PosixSessionUser(user="test-user", group="test-group"),
            runAs="QUEUE_CONFIGURED_USER",
            windows=WindowsSessionUser(user="job-user", passwordArn="dummyvalue"),
        )

        # WHEN
        result = Queue.create(
            client=mock_client,
            display_name=display_name,
            farm=farm,
            role_arn=role_arn,
            job_attachments=job_attachments,
            job_run_as_user=job_run_as_user,
        )

        # THEN
        assert result.id == queue_id
        mock_client.create_queue.assert_called_once_with(
            displayName=display_name,
            farmId=farm.id,
            roleArn=role_arn,
            jobAttachmentSettings=job_attachments.as_queue_settings(),
            jobRunAsUser=asdict(job_run_as_user),
        )

    def test_delete(self, queue: Queue) -> None:
        # GIVEN
        mock_client = MagicMock()

        # WHEN
        queue.delete(client=mock_client)

        # THEN
        mock_client.delete_queue.assert_called_once_with(queueId=queue.id, farmId=queue.farm.id)


class TestFleet:
    def test_create(self, farm: Farm) -> None:
        # GIVEN
        display_name = "test-fleet"
        fleet_id = "fleet-123"
        configuration: dict = {}
        role_arn = "arn:aws:iam::123456789123:role/TestRole"
        mock_client = MagicMock()
        mock_client.create_fleet.return_value = {"fleetId": fleet_id}

        # WHEN
        with patch.object(Fleet, "wait_for_desired_status") as mock_wait_for_desired_status:
            result = Fleet.create(
                client=mock_client,
                display_name=display_name,
                farm=farm,
                configuration=configuration,
                role_arn=role_arn,
                max_worker_count=1,
            )

        # THEN
        assert result.id == fleet_id
        mock_client.create_fleet.assert_called_once_with(
            farmId=farm.id,
            displayName=display_name,
            roleArn=role_arn,
            configuration=configuration,
            maxWorkerCount=1,
        )
        mock_wait_for_desired_status.assert_called_once_with(
            client=mock_client,
            desired_status="ACTIVE",
            allowed_statuses=set(["CREATE_IN_PROGRESS"]),
            interval_s=10,
            max_retries=6 * 5,  # 5 minutes for CMF fleet creation
        )

    def test_delete(self, fleet: Fleet) -> None:
        # GIVEN
        mock_client = MagicMock()

        # WHEN
        fleet.delete(client=mock_client)

        # THEN
        mock_client.delete_fleet.assert_called_once_with(fleetId=fleet.id, farmId=fleet.farm.id)

    class TestWaitForDesiredStatus:
        def test_waits(self, fleet: Fleet) -> None:
            # GIVEN
            desired_status = "ACTIVE"
            allowed_statuses = set(["CREATE_IN_PROGRESS"])
            mock_client = MagicMock()
            mock_client.get_fleet.side_effect = [
                {"status": "CREATE_IN_PROGRESS"},
                {"status": "ACTIVE"},
            ]

            # WHEN
            fleet.wait_for_desired_status(
                client=mock_client,
                desired_status=desired_status,
                allowed_statuses=allowed_statuses,
            )

            # THEN
            mock_client.get_fleet.assert_has_calls(
                [call(fleetId=fleet.id, farmId=fleet.farm.id)] * 2
            )

        def test_raises_when_nonvalid_status_is_reached(self, fleet: Fleet) -> None:
            # GIVEN
            desired_status = "ACTIVE"
            allowed_statuses = set(["CREATE_IN_PROGRESS"])
            mock_client = MagicMock()
            mock_client.get_fleet.side_effect = [
                {"status": "BAD"},
            ]

            with pytest.raises(ValueError) as raised_err:
                # WHEN
                fleet.wait_for_desired_status(
                    client=mock_client,
                    desired_status=desired_status,
                    allowed_statuses=allowed_statuses,
                )

            # THEN
            assert (
                str(raised_err.value)
                == "fleet entered a nonvalid status (BAD) while waiting for the desired status: ACTIVE"
            )


class TestQueueFleetAssociation:
    def test_create(self, farm: Farm, queue: Queue, fleet: Fleet) -> None:
        # GIVEN
        mock_client = MagicMock()

        # WHEN
        QueueFleetAssociation.create(
            client=mock_client,
            farm=farm,
            queue=queue,
            fleet=fleet,
        )

        # THEN
        mock_client.create_queue_fleet_association.assert_called_once_with(
            farmId=farm.id,
            queueId=queue.id,
            fleetId=fleet.id,
        )

    @pytest.mark.parametrize(
        "stop_mode",
        [
            "STOP_SCHEDULING_AND_CANCEL_TASKS",
            "STOP_SCHEDULING_AND_FINISH_TASKS",
        ],
    )
    def test_delete(self, stop_mode: Any, qfa: QueueFleetAssociation) -> None:
        # GIVEN
        mock_client = MagicMock()

        # WHEN
        with patch.object(qfa, "stop") as mock_stop:
            qfa.delete(client=mock_client, stop_mode=stop_mode)

        # THEN
        mock_client.delete_queue_fleet_association.assert_called_once_with(
            farmId=qfa.farm.id,
            queueId=qfa.queue.id,
            fleetId=qfa.fleet.id,
        )
        mock_stop.assert_called_once_with(
            client=mock_client,
            stop_mode=stop_mode,
        )

    class TestStop:
        @pytest.mark.parametrize(
            "stop_mode",
            [
                "STOP_SCHEDULING_AND_CANCEL_TASKS",
                "STOP_SCHEDULING_AND_FINISH_TASKS",
            ],
        )
        def test_stops(self, stop_mode: Any, qfa: QueueFleetAssociation) -> None:
            # GIVEN
            mock_client = MagicMock()
            mock_client.get_queue_fleet_association.side_effect = [
                {"status": stop_mode},
                {"status": "STOPPED"},
            ]

            # WHEN
            qfa.stop(client=mock_client, stop_mode=stop_mode)

            # THEN
            mock_client.update_queue_fleet_association.assert_called_once_with(
                farmId=qfa.farm.id,
                queueId=qfa.queue.id,
                fleetId=qfa.fleet.id,
                status=stop_mode,
            )
            mock_client.get_queue_fleet_association.assert_has_calls(
                [call(farmId=qfa.farm.id, queueId=qfa.queue.id, fleetId=qfa.fleet.id)] * 2
            )

        def test_raises_when_nonvalid_status_is_reached(self, qfa: QueueFleetAssociation) -> None:
            # GIVEN
            mock_client = MagicMock()
            mock_client.get_queue_fleet_association.side_effect = [
                {"status": "BAD"},
            ]

            with pytest.raises(ValueError) as raised_err:
                # WHEN
                qfa.stop(
                    client=mock_client,
                    stop_mode="STOP_SCHEDULING_AND_CANCEL_TASKS",
                )

            # THEN
            assert (
                str(raised_err.value)
                == "Association entered a nonvalid status (BAD) while waiting for the desired status: STOPPED"
            )


class TestJob:
    @staticmethod
    def task_run_status_counts(
        pending: int = 0,
        ready: int = 0,
        assigned: int = 0,
        scheduled: int = 0,
        interrupting: int = 0,
        running: int = 0,
        suspended: int = 0,
        canceled: int = 0,
        failed: int = 0,
        succeeded: int = 0,
    ) -> dict:
        return {
            "PENDING": pending,
            "READY": ready,
            "ASSIGNED": assigned,
            "SCHEDULED": scheduled,
            "INTERRUPTING": interrupting,
            "RUNNING": running,
            "SUSPENDED": suspended,
            "CANCELED": canceled,
            "FAILED": failed,
            "SUCCEEDED": succeeded,
        }

    def test_submit(
        self,
        farm: Farm,
        queue: Queue,
    ) -> None:
        """
        Verifies that Job.submit creates the job, retrieves its details, and returns the expected Job object.
        Note that for the GetJob call, only those that are relevant for Job creation are included. Full testing
        of GetJob calls is covered by the test for Job.get_job_details
        """
        # GIVEN
        # CreateJob parameters
        template = {
            "specificationVersion": "2022-09-01",
            "name": "Test Job",
            "parameters": [
                {
                    "name": "Text",
                    "type": "STRING",
                },
            ],
            "steps": [
                {
                    "name": "Step0",
                    "script": {
                        "actions": {
                            "onRun": {"command": "/bin/echo", "args": [r"{{ Param.Text }}"]}
                        },
                    },
                },
            ],
        }
        priority = 1
        parameters = {"Text": {"string": "Hello world"}}
        target_task_run_status = "SUSPENDED"
        max_failed_tasks_count = 0
        max_retries_per_task = 0

        mock_client = MagicMock()

        # CreateJob mock
        job_id = "job-123"
        mock_client.create_job.return_value = {"jobId": job_id}

        # GetJob mock
        task_run_status_counts = TestJob.task_run_status_counts(
            **{target_task_run_status.lower(): 1}
        )
        created_at = datetime.datetime.now()
        mock_client.get_job.return_value = {
            "jobId": job_id,
            "name": "Test Job",
            "lifecycleStatus": "CREATE_COMPLETE",
            "lifecycleStatusMessage": "Nice",
            "priority": priority,
            "createdAt": created_at,
            "createdBy": "test-user",
            "taskRunStatus": target_task_run_status,
            "taskRunStatusCounts": task_run_status_counts,
            "maxFailedTasksCount": max_failed_tasks_count,
            "maxRetriesPerTask": max_retries_per_task,
            "parameters": parameters,
        }

        # WHEN
        job = Job.submit(
            client=mock_client,
            farm=farm,
            queue=queue,
            template=template,
            priority=priority,
            parameters=parameters,
            target_task_run_status=target_task_run_status,
            max_failed_tasks_count=max_failed_tasks_count,
            max_retries_per_task=max_retries_per_task,
        )

        # THEN
        mock_client.create_job.assert_called_once_with(
            farmId=farm.id,
            queueId=queue.id,
            template=json.dumps(template),
            templateType="JSON",
            priority=priority,
            parameters=parameters,
            targetTaskRunStatus=target_task_run_status,
            maxFailedTasksCount=max_failed_tasks_count,
            maxRetriesPerTask=max_retries_per_task,
        )
        mock_client.get_job.assert_called_once_with(
            farmId=farm.id,
            queueId=queue.id,
            jobId=job_id,
        )
        assert job.id == job_id
        assert job.farm is farm
        assert job.queue is queue
        assert job.template == template
        assert job.name == "Test Job"
        assert job.lifecycle_status == mod.JobLifecycleStatus.CREATE_COMPLETE
        assert job.lifecycle_status_message == "Nice"
        assert job.priority == priority
        assert job.created_at == created_at
        assert job.created_by == "test-user"
        assert job.task_run_status == target_task_run_status
        assert job.task_run_status_counts == task_run_status_counts
        assert job.max_failed_tasks_count == max_failed_tasks_count
        assert job.max_retries_per_task == max_retries_per_task
        assert job.parameters == parameters

    def test_get_job_details(self, farm: Farm, queue: Queue) -> None:
        """
        Verifies that Job.get_job_details correctly maps the GetJob response to
        kwargs that are compatible with Job.__init__
        """
        # GIVEN
        now = datetime.datetime.now()
        job_id = "job-123"
        response = {
            "jobId": job_id,
            "name": "Job Name",
            "lifecycleStatus": "CREATE_COMPLETE",
            "lifecycleStatusMessage": "Nice",
            "priority": 1,
            "createdAt": now - datetime.timedelta(hours=1),
            "createdBy": "User A",
            "updatedAt": now - datetime.timedelta(minutes=30),
            "updatedBy": "User B",
            "startedAt": now - datetime.timedelta(minutes=15),
            "endedAt": now - datetime.timedelta(minutes=1),
            # Just need to test all fields.. this is not reflective of a real job state
            "taskRunStatus": "RUNNING",
            "targetTaskRunStatus": "SUCCEEDED",
            "taskRunStatusCounts": TestJob.task_run_status_counts(running=2, succeeded=8),
            "storageProfileId": "storage-profile-id-123",
            "maxFailedTasksCount": 3,
            "maxRetriesPerTask": 1,
            "parameters": {"ParamA": {"int": "1"}},
            "attachments": {
                "manifests": [
                    {
                        "rootPath": "/root",
                        "rootPathFormat": "posix",
                    },
                ],
                "fileSystem": "COPIED",
            },
            "description": "Testing",
        }
        mock_client = MagicMock()
        mock_client.get_job.return_value = response

        # WHEN
        kwargs = Job.get_job_details(client=mock_client, farm=farm, queue=queue, job_id=job_id)

        # THEN
        # Verify kwargs are parsed/transformed correctly
        assert kwargs["id"] == job_id
        assert kwargs["name"] == response["name"]
        assert kwargs["lifecycle_status"] == response["lifecycleStatus"]
        assert kwargs["lifecycle_status_message"] == response["lifecycleStatusMessage"]
        assert kwargs["priority"] == response["priority"]
        assert kwargs["created_at"] == response["createdAt"]
        assert kwargs["created_by"] == response["createdBy"]
        assert kwargs["updated_at"] == response["updatedAt"]
        assert kwargs["updated_by"] == response["updatedBy"]
        assert kwargs["started_at"] == response["startedAt"]
        assert kwargs["ended_at"] == response["endedAt"]
        assert kwargs["task_run_status"] == TaskStatus[cast(str, response["taskRunStatus"])]
        assert (
            kwargs["target_task_run_status"]
            == TaskStatus[cast(str, response["targetTaskRunStatus"])]
        )
        assert kwargs["task_run_status_counts"] == {
            TaskStatus[k]: v for k, v in cast(dict, response["taskRunStatusCounts"]).items()
        }
        assert kwargs["storage_profile_id"] == response["storageProfileId"]
        assert kwargs["max_failed_tasks_count"] == response["maxFailedTasksCount"]
        assert kwargs["max_retries_per_task"] == response["maxRetriesPerTask"]
        assert kwargs["parameters"] == response["parameters"]
        assert kwargs["attachments"] == response["attachments"]
        assert kwargs["description"] == response["description"]

        # Verify Job.__init__ accepts the kwargs
        try:
            Job(farm=farm, queue=queue, template={}, **kwargs)
        except TypeError as e:
            pytest.fail(f"Job.__init__ did not accept kwargs from Job.get_job_details: {e}")
        else:
            # Success
            pass

    def test_refresh_job_info(self, job: Job) -> None:
        # GIVEN
        # Copy job object for comparison later
        original_job = replace(job)

        # Mock GetJob
        get_job_response = {
            "jobId": job.id,
            "name": job.name,
            "lifecycleStatus": job.lifecycle_status.value,
            "lifecycleStatusMessage": job.lifecycle_status_message,
            "createdAt": job.created_at,
            "createdBy": job.created_by,
            # Change the priority
            "priority": 2,
        }
        mock_client = MagicMock()
        mock_client.get_job.return_value = get_job_response

        # WHEN
        job.refresh_job_info(client=mock_client)

        # THEN
        mock_client.get_job.assert_called_once()

        # Verify priority changed...
        assert job.priority == 2

        # ...and everything else stayed the same
        assert job.name == original_job.name
        assert job.lifecycle_status == original_job.lifecycle_status
        assert job.lifecycle_status_message == original_job.lifecycle_status_message
        assert job.created_at == original_job.created_at
        assert job.created_by == original_job.created_by

    def test_update(self, job: Job) -> None:
        # GIVEN
        priority = 24
        target_task_run_status = "READY"
        max_failed_tasks_count = 1
        max_retries_per_task = 10
        mock_client = MagicMock()

        # WHEN
        job.update(
            client=mock_client,
            priority=priority,
            target_task_run_status=target_task_run_status,
            max_failed_tasks_count=max_failed_tasks_count,
            max_retries_per_task=max_retries_per_task,
        )

        # THEN
        mock_client.update_job.assert_called_once_with(
            farmId=job.farm.id,
            queueId=job.queue.id,
            jobId=job.id,
            priority=priority,
            targetTaskRunStatus=target_task_run_status,
            maxFailedTasksCount=max_failed_tasks_count,
            maxRetriesPerTask=max_retries_per_task,
        )

    def test_wait_until_complete(self, job: Job) -> None:
        # GIVEN
        common_response_kwargs = {
            "jobId": job.id,
            "name": job.name,
            "lifecycleStatus": job.lifecycle_status,
            "lifecycleStatusMessage": job.lifecycle_status_message,
            "priority": job.priority,
            "createdAt": job.created_at,
            "createdBy": job.created_by,
        }
        mock_client = MagicMock()
        mock_client.get_job.side_effect = [
            {
                **common_response_kwargs,
                "taskRunStatus": "RUNNING",
            },
            {
                **common_response_kwargs,
                "taskRunStatus": "FAILED",
            },
        ]

        # WHEN
        job.wait_until_complete(client=mock_client, max_retries=1)

        # THEN
        assert mock_client.get_job.call_count == 2
        assert job.task_run_status == "FAILED"

    def test_get_logs(self, job: Job) -> None:
        # GIVEN
        mock_deadline_client = MagicMock()
        mock_deadline_client.list_sessions.side_effect = [
            {
                "sessions": [
                    {"sessionId": "session-1"},
                ],
                "nextToken": "1",
            },
            {
                "sessions": [
                    {"sessionId": "session-2"},
                ],
            },
        ]
        mock_logs_client = MagicMock()
        log_events: list = [
            {
                "events": [
                    {
                        "ingestionTime": 123,
                        "timestamp": 321,
                        "message": "test",
                    }
                ],
                "nextToken": "a",
            },
            {
                "events": [
                    {
                        "ingestionTime": 123123,
                        "timestamp": 321321,
                        "message": "testtest",
                    }
                ],
            },
        ]
        mock_logs_paginator = mock_logs_client.get_paginator.return_value
        mock_logs_paginator.paginate.return_value.build_full_result.side_effect = log_events

        # WHEN
        job_logs = job.get_logs(
            deadline_client=mock_deadline_client,
            logs_client=mock_logs_client,
        )

        # THEN
        mock_deadline_client.list_sessions.assert_has_calls(
            [
                call(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                ),
                call(
                    farmId=job.farm.id,
                    queueId=job.queue.id,
                    jobId=job.id,
                    nextToken="1",
                ),
            ]
        )
        mock_logs_client.get_paginator.assert_called_once_with("filter_log_events")
        mock_logs_paginator.paginate.assert_has_calls(
            [
                call(
                    logGroupName=f"/aws/deadline/{job.farm.id}/{job.queue.id}",
                    logStreamNames=["session-1"],
                ),
                call().build_full_result(),
                call(
                    logGroupName=f"/aws/deadline/{job.farm.id}/{job.queue.id}",
                    logStreamNames=["session-2"],
                ),
                call().build_full_result(),
            ]
        )

        session_log_map = job_logs.logs
        assert "session-1" in session_log_map
        assert session_log_map["session-1"] == [
            CloudWatchLogEvent.from_api_response(le) for le in log_events[0]["events"]
        ]
        assert "session-2" in session_log_map
        assert session_log_map["session-2"] == [
            CloudWatchLogEvent.from_api_response(le) for le in log_events[1]["events"]
        ]

    def test_get_only_task_fail_on_multi_task(self, job: Job) -> None:
        # GIVEN
        deadline_client = MagicMock()
        step = MagicMock()
        task = MagicMock()
        step.list_tasks.return_value = [task, task]
        task.get_last_session.return_value = session

        with (patch.object(job, "list_steps", return_value=[step]) as mock_list_steps,):

            # WHEN
            def when():
                job.get_only_task(deadline_client=deadline_client)

            # THEN
            with pytest.raises(AssertionError) as raise_ctx:
                when()

        print(raise_ctx.value)

        assert raise_ctx.match("Job contains multiple tasks")
        mock_list_steps.assert_called_once_with(deadline_client=deadline_client)
        step.list_tasks.assert_called_once_with(deadline_client=deadline_client)
        task.get_last_session.assert_not_called()

    def test_get_only_task_fail_on_multi_step(self, job: Job) -> None:
        # GIVEN
        deadline_client = MagicMock()
        step = MagicMock()

        with (patch.object(job, "list_steps", return_value=[step, step]) as mock_list_steps,):
            # WHEN
            def when():
                job.get_only_task(deadline_client=deadline_client)

            # THEN
            with pytest.raises(AssertionError) as raise_ctx:
                when()

        print(raise_ctx.value)

        assert raise_ctx.match("Job contains multiple steps")
        mock_list_steps.assert_called_once_with(deadline_client=deadline_client)
        step.list_tasks.assert_not_called()

    def test_assert_single_task_log_contains_success(self, job: Job, session: Session) -> None:
        # GIVEN
        deadline_client = MagicMock()
        logs_client = MagicMock()
        step = MagicMock()
        task = MagicMock()
        step.list_tasks.return_value = [task]
        task.get_last_session.return_value = session
        expected_pattern = re.compile(r"a message")

        with (
            patch.object(job, "list_steps", return_value=[step]) as mock_list_steps,
            patch.object(session, "assert_log_contains") as mock_session_assert_log_contains,
        ):

            # WHEN
            job.assert_single_task_log_contains(
                deadline_client=deadline_client,
                logs_client=logs_client,
                expected_pattern=expected_pattern,
            )

        # THEN
        # This test is only to confirm that no assertion is raised, since the expected message
        # is in the logs
        mock_session_assert_log_contains.assert_called_once_with(
            logs_client=logs_client,
            expected_pattern=expected_pattern,
            assert_fail_msg="Expected message not found in session log",
            backoff_factor=datetime.timedelta(milliseconds=300),
            retries=6,
        )
        mock_list_steps.assert_called_once_with(deadline_client=deadline_client)
        step.list_tasks.assert_called_once_with(deadline_client=deadline_client)
        task.get_last_session.assert_called_once_with(deadline_client=deadline_client)

    def test_assert_single_task_log_does_not_contain_success(
        self, job: Job, session: Session
    ) -> None:
        # GIVEN
        deadline_client = MagicMock()
        logs_client = MagicMock()
        step = MagicMock()
        task = MagicMock()
        step.list_tasks.return_value = [task]
        task.get_last_session.return_value = session
        expected_pattern = re.compile(r"a message")

        with (
            patch.object(job, "list_steps", return_value=[step]) as mock_list_steps,
            patch.object(
                session, "assert_log_does_not_contain"
            ) as mock_session_assert_log_does_not_contain,
        ):

            # WHEN
            job.assert_single_task_log_does_not_contain(
                deadline_client=deadline_client,
                logs_client=logs_client,
                expected_pattern=expected_pattern,
            )

        # THEN
        # This test is only to confirm that no assertion is raised, since the expected message
        # is in the logs
        mock_session_assert_log_does_not_contain.assert_called_once_with(
            logs_client=logs_client,
            expected_pattern=expected_pattern,
            assert_fail_msg="Expected message found in session log",
            consistency_wait_time=timedelta(seconds=3),
        )
        mock_list_steps.assert_called_once_with(deadline_client=deadline_client)
        step.list_tasks.assert_called_once_with(deadline_client=deadline_client)
        task.get_last_session.assert_called_once_with(deadline_client=deadline_client)

    def test_list_steps(
        self,
        job: Job,
    ) -> None:
        # GIVEN
        step_id = "step-97f70ac0e02d4dc0acb589b9bd890981"
        step_name = "a step"
        created_at = datetime.datetime(2024, 9, 3)
        created_by = "username"
        lifecycle_status = "CREATE_COMPLETE"
        task_run_status = "ASSIGNED"
        lifecycle_status_message = ("a message",)
        target_task_run_status = ("READY",)
        updated_at = datetime.datetime(2024, 9, 3)
        updated_by = "someone"
        started_at = datetime.datetime(2024, 9, 3)
        ended_at = datetime.datetime(2024, 9, 3)
        task_run_status_counts = {
            "PENDING": 0,
            "READY": 0,
            "ASSIGNED": 0,
            "STARTING": 0,
            "SCHEDULED": 0,
            "INTERRUPTING": 0,
            "RUNNING": 0,
            "SUSPENDED": 0,
            "CANCELED": 0,
            "FAILED": 0,
            "SUCCEEDED": 0,
            "NOT_COMPATIBLE": 0,
        }
        dependency_counts = {
            "consumersResolved": 0,
            "consumersUnresolved": 0,
            "dependenciesResolved": 0,
            "dependenciesUnresolved": 0,
        }

        deadline_client = MagicMock()
        deadline_client.get_paginator.return_value.paginate.return_value = [
            {
                "steps": [
                    {
                        "stepId": step_id,
                        "name": step_name,
                        "createdAt": created_at,
                        "createdBy": created_by,
                        "lifecycleStatus": lifecycle_status,
                        "taskRunStatus": task_run_status,
                        "taskRunStatusCounts": task_run_status_counts,
                        "lifeCycleStatusMessage": lifecycle_status_message,
                        "targetTaskRunStatus": target_task_run_status,
                        "updatedAt": updated_at,
                        "updatedBy": updated_by,
                        "startedAt": started_at,
                        "endedAt": ended_at,
                        "dependencyCounts": dependency_counts,
                    },
                ],
            }
        ]
        result = job.list_steps(deadline_client=deadline_client)

        # WHEN
        result_list = list(result)

        # THEN
        assert len(result_list) == 1
        step = result_list[0]
        assert step.id == step_id
        assert step.name == step_name
        assert step.created_at == created_at
        assert step.created_by == created_by
        assert step.lifecycle_status == mod.StepLifecycleStatus(lifecycle_status)
        assert step.task_run_status == task_run_status
        assert step.lifecycle_status_message == lifecycle_status_message
        assert step.target_task_run_status == target_task_run_status
        assert step.updated_at == updated_at
        assert step.updated_by == updated_by
        assert step.started_at == started_at
        assert step.ended_at == ended_at

        for status in step.task_run_status_counts:
            assert step.task_run_status_counts[status] == task_run_status_counts[status.value]

        assert step.dependency_counts is not None
        assert step.dependency_counts.consumers_resolved == dependency_counts["consumersResolved"]
        assert (
            step.dependency_counts.consumers_unresolved == dependency_counts["consumersUnresolved"]
        )
        assert (
            step.dependency_counts.dependencies_resolved
            == dependency_counts["dependenciesResolved"]
        )
        assert (
            step.dependency_counts.dependencies_unresolved
            == dependency_counts["dependenciesUnresolved"]
        )


class TestStep:
    def test_list_tasks(
        self,
        step: Step,
        session_action_id: str,
    ) -> None:
        # GIVEN
        deadline_client = MagicMock()
        mock_get_paginator: MagicMock = deadline_client.get_paginator
        mock_paginate: MagicMock = mock_get_paginator.return_value.paginate
        task_id = "task-b73de3af607f472687cafb16def7664e"
        created_at = datetime.datetime.now()
        created_by = "someone"
        run_status = "READY"
        failure_retry_count = 5
        target_task_run_status = "RUNNING"
        updated_at = datetime.datetime.now()
        updated_by = "someoneelse"
        started_at = datetime.datetime.now()
        ended_at = datetime.datetime.now()
        mock_paginate.return_value = [
            {
                "tasks": [
                    {
                        "taskId": task_id,
                        "createdAt": created_at,
                        "createdBy": created_by,
                        "runStatus": run_status,
                        "failureRetryCount": failure_retry_count,
                        "latestSessionActionId": session_action_id,
                        "parameters": {},
                        "targetTaskRunStatus": target_task_run_status,
                        "updatedAt": updated_at,
                        "updatedBy": updated_by,
                        "startedAt": started_at,
                        "endedAt": ended_at,
                    },
                ],
            },
        ]
        generator = step.list_tasks(deadline_client=deadline_client)

        # WHEN
        result = list(generator)

        # THEN
        assert len(result) == 1
        task = result[0]
        assert task.id == task_id
        assert task.created_at == created_at
        assert task.created_by == created_by
        assert task.run_status == TaskStatus(run_status)
        assert task.failure_retry_count == failure_retry_count
        assert task.latest_session_action_id == session_action_id
        assert task.parameters == {}
        assert task.target_task_run_status == TaskStatus(target_task_run_status)
        assert task.updated_at == updated_at
        assert task.updated_by == updated_by
        assert task.started_at == started_at
        assert task.ended_at == ended_at
        mock_get_paginator.assert_called_once_with("list_tasks")
        mock_paginate.assert_called_once_with(
            farmId=step.farm.id,
            queueId=step.queue.id,
            jobId=step.job.id,
            stepId=step.id,
        )


class TestTask:
    def test_get_last_session(
        self,
        fleet_id: str,
        task: Task,
        session_id: str,
        session_lifecycle_status: mod.SessionLifecycleStatus,
        session_log_config: mod.LogConfiguration,
        worker_id: str,
        worker_log_config: mod.LogConfiguration,
        ec2_instance_arn: str,
        ec2_instance_type: str,
        host_name: str,
        ip_v4_addresses: list[str],
        ip_v6_addresses: list[str],
    ) -> None:
        # GIVEN
        deadline_client = MagicMock()
        mock_get_session: MagicMock = deadline_client.get_session
        started_at = datetime.datetime.now()
        ended_at = datetime.datetime.now()
        updated_at = datetime.datetime.now()
        updated_by = "taskupdater"
        mock_get_session.return_value = {
            "sessionId": session_id,
            "fleetId": fleet_id,
            "lifecycleStatus": session_lifecycle_status.value,
            "log": {
                "logDriver": session_log_config.log_driver,
                "options": session_log_config.options,
                "parameters": session_log_config.parameters,
            },
            "hostProperties": {
                "ec2InstanceArn": ec2_instance_arn,
                "ec2InstanceType": ec2_instance_type,
                "hostName": host_name,
                "ipAddresses": {
                    "ipV4Addresses": ip_v4_addresses,
                    "ipV6Addresses": ip_v6_addresses,
                },
            },
            "startedAt": started_at,
            "endedAt": ended_at,
            "updatedAt": updated_at,
            "updatedBy": updated_by,
            "workerId": worker_id,
            "workerLog": {
                "logDriver": worker_log_config.log_driver,
                "options": worker_log_config.options,
                "parameters": worker_log_config.parameters,
            },
        }

        # WHEN
        returned_session = task.get_last_session(deadline_client=deadline_client)

        # THEN
        assert isinstance(returned_session, Session)
        assert returned_session.id == session_id
        assert returned_session.fleet.id == fleet_id
        assert returned_session.worker_id == worker_id
        assert returned_session.lifecycle_status == session_lifecycle_status
        assert returned_session.started_at == started_at
        assert returned_session.ended_at == ended_at
        assert returned_session.updated_at == updated_at
        assert returned_session.updated_by == updated_by

        assert isinstance(returned_session.host_properties, mod.WorkerHostProperties)
        assert returned_session.host_properties.ec2_instance_arn == ec2_instance_arn
        assert returned_session.host_properties.ec2_instance_type == ec2_instance_type
        assert returned_session.host_properties.host_name == host_name

        assert isinstance(returned_session.host_properties.ip_addresses, mod.IpAddresses)
        assert returned_session.host_properties.ip_addresses.ip_v4_addresses == ip_v4_addresses
        assert returned_session.host_properties.ip_addresses.ip_v6_addresses == ip_v6_addresses

        assert isinstance(returned_session.logs, mod.LogConfiguration)
        assert returned_session.logs.log_driver == session_log_config.log_driver
        assert returned_session.logs.options == session_log_config.options
        assert returned_session.logs.parameters == session_log_config.parameters

        assert isinstance(returned_session.worker_log, mod.LogConfiguration)
        assert returned_session.worker_log.log_driver == worker_log_config.log_driver
        assert returned_session.worker_log.options == worker_log_config.options
        assert returned_session.worker_log.parameters == worker_log_config.parameters


class TestSession:

    base_assertion_args = (
        ("expected_pattern", "log_messages"),
        (
            pytest.param("PATTERN", ["PATTERN"], id="exact-match"),
            pytest.param("PATTERN", ["PATTERN at beginning"], id="match-beginning"),
            pytest.param("PATTERN", ["ends with PATTERN"], id="match-end"),
            pytest.param("PATTERN", ["multiline with", "the PATTERN"], id="multi-line"),
            pytest.param(
                re.compile(r"This is\na multiline pattern", re.MULTILINE),
                ["extra lines", "This is", "a multiline pattern", "embedded"],
                id="multi-line-pattern",
            ),
            pytest.param(
                re.compile(r"^anchored\nmultiline pattern", re.MULTILINE),
                ["extra lines", "anchored", "multiline pattern", "trailing line"],
                id="anchored-multi-line-pattern",
            ),
        ),
    )

    @pytest.mark.parametrize(*base_assertion_args)
    def test_assert_logs_success(
        self,
        session: Session,
        expected_pattern: str | re.Pattern,
        log_messages: list[str],
        log_client,
    ) -> None:
        # GIVEN
        logs_client, logs = log_client(session, log_messages)

        with (
            patch.object(session, "get_session_log", return_value=logs) as mock_get_session_log,
            # Speed up tests
            patch.object(mod.time, "sleep") as mock_time_sleep,
        ):

            # WHEN
            session.assert_log_contains(
                logs_client=logs_client,
                expected_pattern=expected_pattern,
            )

        # THEN
        # (no exception is raised)
        mock_get_session_log.assert_called_once_with(logs_client=logs_client)
        mock_time_sleep.assert_not_called()

    @pytest.mark.parametrize(*base_assertion_args)
    def test_assert_logs_does_not_contain_fail(
        self,
        session: Session,
        expected_pattern: str | re.Pattern,
        log_messages: list[str],
        log_client,
    ) -> None:
        # GIVEN
        logs_client, logs = log_client(session, log_messages)
        expected_assertion_msg = (
            "Expected message found in session log."
            " Logs are in CloudWatch log group: sessionLogGroup"
        )
        with (
            patch.object(session, "get_session_log", return_value=logs) as mock_get_session_log,
            # Speed up tests
            patch.object(mod.time, "sleep") as mock_time_sleep,
        ):

            # WHEN
            def when():
                session.assert_log_does_not_contain(
                    logs_client=logs_client,
                    expected_pattern=expected_pattern,
                )

            # THEN
            with pytest.raises(AssertionError) as raise_ctx:
                when()
            assert raise_ctx.value.args[0] == expected_assertion_msg
            mock_get_session_log.assert_called_once_with(logs_client=logs_client)
            mock_time_sleep.assert_not_called()

    @pytest.mark.parametrize(
        argnames=("expected_pattern", "log_messages"),
        argvalues=(
            pytest.param("DOES NOT MATCH", ["PATTERN"], id="no-match"),
            pytest.param(
                re.compile(r"There is\nno match", re.MULTILINE),
                ["extra lines", "This is", "a multiline pattern", "embedded"],
                id="multi-line-no-match",
            ),
        ),
    )
    def test_assert_logs_does_not_contain_success(
        self,
        session: Session,
        expected_pattern: str | re.Pattern,
        log_messages: list[str],
        log_client,
    ) -> None:
        # GIVEN
        logs_client, logs = log_client(session, log_messages)

        with (
            patch.object(session, "get_session_log", return_value=logs) as mock_get_session_log,
            # Speed up tests
            patch.object(mod.time, "sleep") as mock_time_sleep,
        ):

            # WHEN
            session.assert_log_does_not_contain(
                logs_client=logs_client,
                expected_pattern=expected_pattern,
            )

        # THEN
        # (no exception is raised)
        mock_get_session_log.assert_has_calls([call(logs_client=logs_client)] * 2)
        mock_time_sleep.assert_called_once_with(4.5)  # 4.5 is default sleep duration

    @pytest.mark.parametrize(
        argnames=("sleep_duration"),
        argvalues=(
            pytest.param(timedelta(seconds=9), id="custom-duration"),
            pytest.param(timedelta(seconds=0), id="no-sleep"),
        ),
    )
    def test_assert_logs_does_not_contain_sleeps(
        self, session: Session, sleep_duration: timedelta, log_client
    ) -> None:
        # GIVEN
        logs_client, logs = log_client(session, ["PATTERN"])

        with (
            patch.object(session, "get_session_log", return_value=logs) as mock_get_session_log,
            # Speed up tests
            patch.object(mod.time, "sleep") as mock_time_sleep,
        ):

            # WHEN
            session.assert_log_does_not_contain(
                logs_client=logs_client,
                expected_pattern="DOES NOT MATCH",
                consistency_wait_time=sleep_duration,
            )

        # THEN
        # (no exception is raised)
        if sleep_duration.total_seconds() > 0:
            mock_time_sleep.assert_called_once_with(sleep_duration.total_seconds())
            mock_get_session_log.assert_has_calls([call(logs_client=logs_client)] * 2)
        else:
            mock_get_session_log.assert_called_once_with(logs_client=logs_client)
            mock_time_sleep.assert_not_called()

    @pytest.mark.parametrize(
        argnames="assert_fail_msg",
        argvalues=(
            pytest.param(None, id="default"),
            pytest.param("message to raise", id="provided-assert-msg"),
        ),
    )
    @pytest.mark.parametrize(
        argnames="retries",
        argvalues=(pytest.param(None, id="retries[default]"), pytest.param(3, id="rerties[3]")),
    )
    @pytest.mark.parametrize(
        argnames="backoff_factor",
        argvalues=(
            pytest.param(None, id="backoff_factor[default]"),
            pytest.param(datetime.timedelta(seconds=10), id="backoff_factor[10s]"),
        ),
    )
    def test_assert_logs_contains_fail(
        self,
        session: Session,
        assert_fail_msg: str | None,
        retries: int | None,
        backoff_factor: datetime.timedelta | None,
        session_log_group_name: str,
    ) -> None:
        # GIVEN
        logs_client = MagicMock()
        session_id = "session-5815c7b8054c4548837c2538f0139661"
        logs = mod.SessionLog(
            session_id=session_id,
            logs=[
                mod.CloudWatchLogEvent(
                    ingestion_time=i,
                    message=message,
                    timestamp=i,
                )
                for i, message in enumerate(
                    [
                        "this is not the expected message",
                    ]
                )
            ],
        )
        expected_assertion_msg = (
            f"{assert_fail_msg or 'Expected message not found in session log'}."
            f" Logs are in CloudWatch log group: {session_log_group_name}"
        )
        expected_retries = retries if retries is not None else 6
        expected_backoff_factor = (
            backoff_factor if backoff_factor is not None else datetime.timedelta(milliseconds=300)
        )

        with (
            patch.object(session, "get_session_log", return_value=logs) as mock_get_session_log,
            # Speed up tests
            patch.object(mod.time, "sleep") as mock_time_sleep,
        ):
            # WHEN
            def when():
                kwargs: dict[str, Any] = {
                    "logs_client": logs_client,
                    "expected_pattern": re.compile("a message"),
                }
                if assert_fail_msg is not None:
                    kwargs["assert_fail_msg"] = assert_fail_msg
                if retries is not None:
                    kwargs["retries"] = retries
                if backoff_factor is not None:
                    kwargs["backoff_factor"] = backoff_factor
                session.assert_log_contains(**kwargs)

            # THEN
            with pytest.raises(AssertionError) as raise_ctx:
                when()

        assert raise_ctx.value.args[0] == expected_assertion_msg
        mock_get_session_log.assert_has_calls(
            [call(logs_client=logs_client)] * (expected_retries + 1)
        )
        assert mock_get_session_log.call_count == (expected_retries + 1)
        mock_time_sleep.assert_has_calls(
            [
                call((expected_backoff_factor * (2**i)).total_seconds())
                for i in range(expected_retries)
            ]
        )
        assert mock_time_sleep.call_count == expected_retries

    @pytest.mark.parametrize(
        argnames="retries_before_success",
        argvalues=(1, 2),
    )
    def test_assert_logs_contain_cw_eventual_consistency(
        self,
        session: Session,
        retries_before_success: int,
    ) -> None:
        # GIVEN
        logs_client = MagicMock()
        session_id = "session-5815c7b8054c4548837c2538f0139661"
        message_only_in_complete_logs = "message only in complete logs"
        partial_log = mod.SessionLog(
            session_id=session_id,
            logs=[
                mod.CloudWatchLogEvent(
                    ingestion_time=0,
                    message="this contains partial logs",
                    timestamp=0,
                ),
            ],
        )
        complete_log = mod.SessionLog(
            session_id=session_id,
            logs=[
                mod.CloudWatchLogEvent(
                    ingestion_time=0,
                    message="this contains partial logs",
                    timestamp=0,
                ),
                mod.CloudWatchLogEvent(
                    ingestion_time=0,
                    message=message_only_in_complete_logs,
                    timestamp=0,
                ),
            ],
        )

        with (
            patch.object(
                session,
                "get_session_log",
                side_effect=(
                    # Return partial log before performing the specified number of retries
                    ([partial_log] * retries_before_success)
                    # The complete log
                    + [complete_log]
                ),
            ) as mock_get_session_log,
            patch.object(mod.time, "sleep") as mock_time_sleep,
        ):

            # WHEN
            session.assert_log_contains(
                logs_client=logs_client,
                expected_pattern=re.compile(re.escape(message_only_in_complete_logs)),
            )

            # THEN
            mock_get_session_log.assert_has_calls(
                [call(logs_client=logs_client)] * (retries_before_success + 1)
            )
            mock_time_sleep.assert_has_calls(
                [call(0.3 * (2.0**i)) for i in range(retries_before_success)]
            )

    @pytest.mark.parametrize(
        argnames="log_event_messages",
        argvalues=(
            pytest.param(["a", "b"], id="2events"),
            pytest.param(["a", "b", "c"], id="3events"),
        ),
    )
    def test_get_session_log(
        self,
        session: Session,
        log_event_messages: list[str],
    ) -> None:
        # GIVEN
        logs_client = MagicMock()
        logs_client.get_paginator.return_value.paginate.return_value.build_full_result.return_value = {
            "events": [
                {
                    "ingestionTime": i,
                    "message": message,
                    "timestamp": i,
                }
                for i, message in enumerate(log_event_messages)
            ]
        }

        # WHEN
        result = session.get_session_log(logs_client=logs_client)

        # THEN
        assert isinstance(result, mod.SessionLog)
        assert result.session_id == session.id
        for log_event, expected_message in zip(result.logs, log_event_messages):
            assert log_event.message == expected_message
