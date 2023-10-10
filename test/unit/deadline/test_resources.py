# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import datetime
import json
from dataclasses import replace
from typing import Any, Generator, cast
from unittest.mock import MagicMock, call, patch

import pytest

from deadline_test_fixtures import (
    CloudWatchLogEvent,
    Farm,
    Queue,
    Fleet,
    QueueFleetAssociation,
    Job,
    JobAttachmentSettings,
    TaskStatus,
)
from deadline_test_fixtures.deadline import resources as mod


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
def fleet(farm: Farm) -> Fleet:
    return Fleet(id="fleet-123", farm=farm)


@pytest.fixture
def qfa(farm: Farm, queue: Queue, fleet: Fleet) -> QueueFleetAssociation:
    return QueueFleetAssociation(
        farm=farm,
        queue=queue,
        fleet=fleet,
    )


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

        # WHEN
        result = Queue.create(
            client=mock_client,
            display_name=display_name,
            farm=farm,
            role_arn=role_arn,
            job_attachments=job_attachments,
        )

        # THEN
        assert result.id == queue_id
        mock_client.create_queue.assert_called_once_with(
            displayName=display_name,
            farmId=farm.id,
            roleArn=role_arn,
            jobAttachmentSettings=job_attachments.as_queue_settings(),
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
            )

        # THEN
        assert result.id == fleet_id
        mock_client.create_fleet.assert_called_once_with(
            farmId=farm.id,
            displayName=display_name,
            roleArn=role_arn,
            configuration=configuration,
        )
        mock_wait_for_desired_status.assert_called_once_with(
            client=mock_client,
            desired_status="ACTIVE",
            allowed_statuses=set(["CREATE_IN_PROGRESS"]),
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

    @pytest.fixture
    def job(self, farm: Farm, queue: Queue) -> Job:
        return Job(
            id="job-123",
            farm=farm,
            queue=queue,
            template={},
            name="Job Name",
            lifecycle_status="CREATE_COMPLETE",
            lifecycle_status_message="Nice",
            priority=1,
            created_at=datetime.datetime.now(),
            created_by="test-user",
        )

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
        assert job.lifecycle_status == "CREATE_COMPLETE"
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
            "lifecycleStatus": job.lifecycle_status,
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
        mock_deadline_client.list_sessions.return_value = {
            "sessions": [
                {"sessionId": "session-1"},
                {"sessionId": "session-2"},
            ],
        }
        mock_logs_client = MagicMock()
        log_events = [
            {
                "events": [
                    {
                        "ingestionTime": 123,
                        "timestamp": 321,
                        "message": "test",
                    }
                ],
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
        mock_logs_client.get_log_events.side_effect = log_events

        # WHEN
        job_logs = job.get_logs(
            deadline_client=mock_deadline_client,
            logs_client=mock_logs_client,
        )

        # THEN
        mock_deadline_client.list_sessions.assert_called_once_with(
            farmId=job.farm.id,
            queueId=job.queue.id,
            jobId=job.id,
        )
        mock_logs_client.get_log_events.assert_has_calls(
            [
                call(
                    logGroupName=f"/aws/deadline/{job.farm.id}/{job.queue.id}",
                    logStreamName=session_id,
                )
                for session_id in ["session-1", "session-2"]
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
