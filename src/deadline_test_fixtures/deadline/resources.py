# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import datetime
import json
import logging
import re
import time
from collections.abc import Generator
from dataclasses import asdict, dataclass, fields
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, Union

from botocore.client import BaseClient

from ..models import JobAttachmentSettings, JobRunAsUser
from ..util import call_api, clean_kwargs, wait_for
from .client import DeadlineClient

if TYPE_CHECKING:
    from botocore.paginate import PageIterator, Paginator

LOG = logging.getLogger(__name__)


@dataclass
class Farm:
    id: str

    @staticmethod
    def create(
        *,
        client: DeadlineClient,
        display_name: str,
        raw_kwargs: dict | None = None,
    ) -> Farm:
        response = call_api(
            description=f"Create farm {display_name}",
            fn=lambda: client.create_farm(
                displayName=display_name,
                **(raw_kwargs or {}),
            ),
        )
        farm_id = response["farmId"]
        LOG.info(f"Created farm: {farm_id}")
        return Farm(id=farm_id)

    def delete(self, *, client: DeadlineClient, raw_kwargs: dict | None = None) -> None:
        call_api(
            description=f"Delete farm {self.id}",
            fn=lambda: client.delete_farm(farmId=self.id, **(raw_kwargs or {})),
        )


@dataclass
class Queue:
    id: str
    farm: Farm

    @staticmethod
    def create(
        *,
        client: DeadlineClient,
        display_name: str,
        farm: Farm,
        job_run_as_user: JobRunAsUser | None = None,
        role_arn: str | None = None,
        job_attachments: JobAttachmentSettings | None = None,
        raw_kwargs: dict | None = None,
    ) -> Queue:
        kwargs = clean_kwargs(
            {
                "displayName": display_name,
                "farmId": farm.id,
                "roleArn": role_arn,
                "jobAttachmentSettings": (
                    job_attachments.as_queue_settings() if job_attachments else None
                ),
                "jobRunAsUser": asdict(job_run_as_user) if job_run_as_user else None,
                **(raw_kwargs or {}),
            }
        )

        response = call_api(
            description=f"Create queue {display_name} in farm {farm.id}",
            fn=lambda: client.create_queue(**kwargs),
        )

        queue_id = response["queueId"]
        LOG.info(f"Created queue: {queue_id}")
        return Queue(
            id=queue_id,
            farm=farm,
        )

    def delete(self, *, client: DeadlineClient, raw_kwargs: dict | None = None) -> None:
        call_api(
            description=f"Delete queue {self.id}",
            fn=lambda: client.delete_queue(
                queueId=self.id, farmId=self.farm.id, **(raw_kwargs or {})
            ),
        )


@dataclass
class Fleet:
    id: str
    farm: Farm
    autoscaling: bool = True

    @staticmethod
    def create(
        *,
        client: DeadlineClient,
        display_name: str,
        farm: Farm,
        configuration: dict,
        max_worker_count: int,
        min_worker_count: int | None = None,
        role_arn: str | None = None,
        raw_kwargs: dict | None = None,
    ) -> Fleet:
        kwargs = clean_kwargs(
            {
                "farmId": farm.id,
                "displayName": display_name,
                "roleArn": role_arn,
                "configuration": configuration,
                "maxWorkerCount": max_worker_count,
                **(raw_kwargs or {}),
            }
        )
        if min_worker_count is not None:
            kwargs["minWorkerCount"] = min_worker_count
        response = call_api(
            fn=lambda: client.create_fleet(**kwargs),
            description=f"Create fleet {display_name} in farm {farm.id}",
        )
        fleet_id = response["fleetId"]
        LOG.info(f"Created fleet: {fleet_id}")
        fleet = Fleet(
            id=fleet_id,
            farm=farm,
        )

        fleet.wait_for_desired_status(
            client=client,
            desired_status="ACTIVE",
            allowed_statuses=set(["CREATE_IN_PROGRESS"]),
            interval_s=10,
            max_retries=6 * 5,  # 5 minutes to allow CMF fleet creation to complete
        )

        return fleet

    def delete(self, *, client: DeadlineClient, raw_kwargs: dict | None = None) -> None:
        call_api(
            description=f"Delete fleet {self.id}",
            fn=lambda: client.delete_fleet(
                farmId=self.farm.id,
                fleetId=self.id,
                **(raw_kwargs or {}),
            ),
        )

    def wait_for_desired_status(
        self,
        *,
        client: DeadlineClient,
        desired_status: str,
        allowed_statuses: set[str] = set(),
        interval_s: int = 10,
        max_retries: int = 6,
    ) -> None:
        valid_statuses = set([desired_status]).union(allowed_statuses)

        # Temporary until we have waiters
        def is_fleet_desired_status() -> bool:
            response = call_api(
                description=f"Get fleet {self.id}",
                fn=lambda: client.get_fleet(fleetId=self.id, farmId=self.farm.id),
            )
            fleet_status = response["status"]

            if fleet_status not in valid_statuses:
                raise ValueError(
                    f"fleet entered a nonvalid status ({fleet_status}) while "
                    f"waiting for the desired status: {desired_status}"
                )

            return fleet_status == desired_status

        wait_for(
            description=f"fleet {self.id} to reach desired status {desired_status}",
            predicate=is_fleet_desired_status,
            interval_s=interval_s,
            max_retries=max_retries,
        )


@dataclass
class QueueFleetAssociation:
    farm: Farm
    queue: Queue
    fleet: Fleet

    @staticmethod
    def create(
        *,
        client: DeadlineClient,
        farm: Farm,
        queue: Queue,
        fleet: Fleet,
        raw_kwargs: dict | None = None,
    ) -> QueueFleetAssociation:
        call_api(
            description=f"Create queue-fleet association for queue {queue.id} and fleet {fleet.id} in farm {farm.id}",
            fn=lambda: client.create_queue_fleet_association(
                farmId=farm.id,
                queueId=queue.id,
                fleetId=fleet.id,
                **(raw_kwargs or {}),
            ),
        )
        return QueueFleetAssociation(
            farm=farm,
            queue=queue,
            fleet=fleet,
        )

    def delete(
        self,
        *,
        client: DeadlineClient,
        stop_mode: Literal[
            "STOP_SCHEDULING_AND_CANCEL_TASKS", "STOP_SCHEDULING_AND_FINISH_TASKS"
        ] = "STOP_SCHEDULING_AND_CANCEL_TASKS",
        raw_kwargs: dict | None = None,
    ) -> None:
        self.stop(client=client, stop_mode=stop_mode)
        call_api(
            description=f"Delete queue-fleet association for queue {self.queue.id} and fleet {self.fleet.id} in farm {self.farm.id}",
            fn=lambda: client.delete_queue_fleet_association(
                farmId=self.farm.id,
                queueId=self.queue.id,
                fleetId=self.fleet.id,
                **(raw_kwargs or {}),
            ),
        )

    def stop(
        self,
        *,
        client: DeadlineClient,
        stop_mode: Literal[
            "STOP_SCHEDULING_AND_CANCEL_TASKS", "STOP_SCHEDULING_AND_FINISH_TASKS"
        ] = "STOP_SCHEDULING_AND_CANCEL_TASKS",
        interval_s: int = 10,
        max_retries: int = 6,
    ) -> None:
        call_api(
            description=f"Set queue-fleet association to STOPPING_SCHEDULING_AND_CANCELING_TASKS for queue {self.queue.id} and fleet {self.fleet.id}",
            fn=lambda: client.update_queue_fleet_association(
                farmId=self.farm.id,
                queueId=self.queue.id,
                fleetId=self.fleet.id,
                status=stop_mode,
            ),
        )

        # Temporary until we have waiters
        valid_statuses = set(["STOPPED", stop_mode])

        def is_qfa_in_desired_status() -> bool:
            response = call_api(
                description=f"Get queue-fleet association for queue {self.queue.id} and fleet {self.fleet.id}",
                fn=lambda: client.get_queue_fleet_association(
                    farmId=self.farm.id,
                    queueId=self.queue.id,
                    fleetId=self.fleet.id,
                ),
            )

            qfa_status = response["status"]
            if qfa_status not in valid_statuses:
                raise ValueError(
                    f"Association entered a nonvalid status ({qfa_status}) while "
                    "waiting for the desired status: STOPPED"
                )

            return qfa_status == "STOPPED"

        wait_for(
            description="queue-fleet association to reach desired status STOPPED",
            predicate=is_qfa_in_desired_status,
            interval_s=interval_s,
            max_retries=max_retries,
        )


class StrEnum(str, Enum):
    pass


class JobLifecycleStatus(StrEnum):
    ARCHIVED = "ARCHIVED"
    CREATE_COMPLETE = "CREATE_COMPLETE"
    CREATE_FAILED = "CREATE_FAILED"
    CREATE_IN_PROGRESS = "CREATE_IN_PROGRESS"
    UPDATE_FAILED = "UPDATE_FAILED"
    UPDATE_IN_PROGRESS = "UPDATE_IN_PROGRESS"
    UPDATE_SUCCEEDED = "UPDATE_SUCCEEDED"
    UPLOAD_FAILED = "UPLOAD_FAILED"
    UPLOAD_IN_PROGRESS = "UPLOAD_IN_PROGRESS"


class StepLifecycleStatus(StrEnum):
    CREATE_COMPLETE = "CREATE_COMPLETE"
    UPDATE_FAILED = "UPDATE_FAILED"
    UPDATE_IN_PROGRESS = "UPDATE_IN_PROGRESS"
    UPDATE_SUCCEEDED = "UPDATE_SUCCEEDED"


class SessionLifecycleStatus(StrEnum):
    ENDED = "ENDED"
    STARTED = "STARTED"
    UPDATE_FAILED = "UPDATE_FAILED"
    UPDATE_IN_PROGRESS = "UPDATE_IN_PROGRESS"
    UPDATE_SUCCEEDED = "UPDATE_SUCCEEDED"


class TaskStatus(StrEnum):
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    ASSIGNED = "ASSIGNED"
    STARTING = "STARTING"
    SCHEDULED = "SCHEDULED"
    INTERRUPTING = "INTERRUPTING"
    SUSPENDED = "SUSPENDED"
    CANCELED = "CANCELED"
    FAILED = "FAILED"
    SUCCEEDED = "SUCCEEDED"
    NOT_COMPATIBLE = "NOT_COMPATIBLE"


COMPLETE_TASK_STATUSES = set(
    (
        TaskStatus.CANCELED,
        TaskStatus.FAILED,
        TaskStatus.SUCCEEDED,
    )
)


@dataclass
class IpAddresses:
    ip_v4_addresses: list[str] | None = None
    ip_v6_addresses: list[str] | None = None

    @staticmethod
    def from_api_response(response: dict[str, Any]) -> IpAddresses:
        return IpAddresses(
            ip_v4_addresses=response.get("ipV4Addresses", None),
            ip_v6_addresses=response.get("ipV6Addresses", None),
        )


@dataclass
class WorkerHostProperties:
    ec2_instance_arn: str | None = None
    ec2_instance_type: str | None = None
    host_name: str | None = None
    ip_addresses: IpAddresses | None = None

    @staticmethod
    def from_api_response(response: dict[str, Any]) -> WorkerHostProperties:
        ip_addresses = response.get("ipAddresses", None)
        return WorkerHostProperties(
            ec2_instance_arn=response.get("ec2InstanceArn", None),
            ec2_instance_type=response.get("ec2InstanceType", None),
            host_name=response.get("hostName", None),
            ip_addresses=IpAddresses.from_api_response(ip_addresses) if ip_addresses else None,
        )


@dataclass
class Job:
    id: str
    farm: Farm
    queue: Queue
    template: dict

    name: str
    lifecycle_status: JobLifecycleStatus
    lifecycle_status_message: str
    priority: int
    created_at: datetime.datetime
    created_by: str

    updated_at: datetime.datetime | None = None
    updated_by: str | None = None
    started_at: datetime.datetime | None = None
    ended_at: datetime.datetime | None = None
    task_run_status: TaskStatus | None = None
    target_task_run_status: TaskStatus | None = None
    task_run_status_counts: dict[TaskStatus, int] | None = None
    storage_profile_id: str | None = None
    max_failed_tasks_count: int | None = None
    max_retries_per_task: int | None = None
    parameters: dict | None = None
    attachments: dict | None = None
    description: str | None = None

    @staticmethod
    def submit(
        *,
        client: DeadlineClient,
        farm: Farm,
        queue: Queue,
        template: dict,
        priority: int,
        parameters: dict | None = None,
        attachments: dict | None = None,
        target_task_run_status: str | None = None,
        max_failed_tasks_count: int | None = None,
        max_retries_per_task: int | None = None,
        raw_kwargs: dict | None = None,
    ) -> Job:
        kwargs = clean_kwargs(
            {
                "farmId": farm.id,
                "queueId": queue.id,
                "template": json.dumps(template),
                "templateType": "JSON",
                "priority": priority,
                "parameters": parameters,
                "attachments": attachments,
                "targetTaskRunStatus": target_task_run_status,
                "maxFailedTasksCount": max_failed_tasks_count,
                "maxRetriesPerTask": max_retries_per_task,
                **(raw_kwargs or {}),
            }
        )
        create_job_response = call_api(
            description=f"Create job in farm {farm.id} and queue {queue.id}",
            fn=lambda: client.create_job(**kwargs),
        )
        job_id = create_job_response["jobId"]
        LOG.info(f"Created job: {job_id}")

        job_details = Job.get_job_details(
            client=client,
            farm=farm,
            queue=queue,
            job_id=job_id,
        )

        return Job(
            farm=farm,
            queue=queue,
            template=template,
            **job_details,
        )

    @staticmethod
    def get_job_details(
        *,
        client: DeadlineClient,
        farm: Farm,
        queue: Queue,
        job_id: str,
        raw_kwargs: dict | None = None,
    ) -> dict[str, Any]:
        """
        Calls GetJob API and returns the parsed response, which can be used as
        keyword arguments to create/update this class.
        """
        response = call_api(
            description=f"Fetching job details for job {job_id}",
            fn=lambda: client.get_job(
                farmId=farm.id,
                queueId=queue.id,
                jobId=job_id,
                **(raw_kwargs or {}),
            ),
        )

        def get_optional_field(
            name: str,
            *,
            default: Any = None,
            transform: Callable[[Any], Any] | None = None,
        ):
            if name not in response:
                return default
            return transform(response[name]) if transform else response[name]

        return {
            "id": response["jobId"],
            "name": response["name"],
            "lifecycle_status": JobLifecycleStatus(response["lifecycleStatus"]),
            "lifecycle_status_message": response["lifecycleStatusMessage"],
            "priority": response["priority"],
            "created_at": response["createdAt"],
            "created_by": response["createdBy"],
            "updated_at": get_optional_field("updatedAt"),
            "updated_by": get_optional_field("updatedBy"),
            "started_at": get_optional_field("startedAt"),
            "ended_at": get_optional_field("endedAt"),
            "task_run_status": get_optional_field(
                "taskRunStatus",
                transform=lambda trs: TaskStatus[trs],
            ),
            "target_task_run_status": get_optional_field(
                "targetTaskRunStatus",
                transform=lambda trs: TaskStatus[trs],
            ),
            "task_run_status_counts": get_optional_field(
                "taskRunStatusCounts",
                transform=lambda trsc: {TaskStatus[k]: v for k, v in trsc.items()},
            ),
            "storage_profile_id": get_optional_field("storageProfileId"),
            "max_failed_tasks_count": get_optional_field("maxFailedTasksCount"),
            "max_retries_per_task": get_optional_field("maxRetriesPerTask"),
            "parameters": get_optional_field("parameters"),
            "attachments": get_optional_field("attachments"),
            "description": get_optional_field("description"),
        }

    def get_logs(
        self,
        *,
        deadline_client: DeadlineClient,
        logs_client: BaseClient,
    ) -> JobLogs:
        """
        Gets the logs for this Job.

        Args:
            deadline_client (DeadlineClient): The DeadlineClient to use
            logs_client (BaseClient): The CloudWatch logs boto client to use

        Returns:
            JobLogs: The job logs
        """

        def paginate_list_sessions():
            response = deadline_client.list_sessions(
                farmId=self.farm.id,
                queueId=self.queue.id,
                jobId=self.id,
            )
            yield response
            while response.get("nextToken"):
                response = deadline_client.list_sessions(
                    farmId=self.farm.id,
                    queueId=self.queue.id,
                    jobId=self.id,
                    nextToken=response["nextToken"],
                )
                yield response

        list_sessions_pages = call_api(
            description=f"Listing sessions for job {self.id}",
            fn=paginate_list_sessions,
        )
        sessions = [s for p in list_sessions_pages for s in p["sessions"]]

        log_group_name = f"/aws/deadline/{self.farm.id}/{self.queue.id}"
        filter_log_events_paginator: Paginator = logs_client.get_paginator("filter_log_events")
        session_log_map: dict[str, list[CloudWatchLogEvent]] = {}
        for session in sessions:
            session_id = session["sessionId"]
            filter_log_events_pages: PageIterator = call_api(
                description=f"Fetching log events for session {session_id} in log group {log_group_name}",
                fn=lambda: filter_log_events_paginator.paginate(
                    logGroupName=log_group_name,
                    logStreamNames=[session_id],
                ),
            )
            log_events = filter_log_events_pages.build_full_result()
            session_log_map[session_id] = [
                CloudWatchLogEvent.from_api_response(e) for e in log_events["events"]
            ]

        return JobLogs(
            log_group_name=log_group_name,
            logs=session_log_map,
        )

    def refresh_job_info(self, *, client: DeadlineClient) -> None:
        """
        Calls GetJob API to refresh job information. The result is used to update the fields
        of this class.
        """
        kwargs = Job.get_job_details(
            client=client,
            farm=self.farm,
            queue=self.queue,
            job_id=self.id,
        )
        all_field_names = set([f.name for f in fields(self)])
        assert all(k in all_field_names for k in kwargs)
        for k, v in kwargs.items():
            object.__setattr__(self, k, v)

    def update(
        self,
        *,
        client: DeadlineClient,
        priority: int | None = None,
        target_task_run_status: str | None = None,
        max_failed_tasks_count: int | None = None,
        max_retries_per_task: int | None = None,
        raw_kwargs: dict | None = None,
    ) -> None:
        kwargs = clean_kwargs(
            {
                "priority": priority,
                "targetTaskRunStatus": target_task_run_status,
                "maxFailedTasksCount": max_failed_tasks_count,
                "maxRetriesPerTask": max_retries_per_task,
                **(raw_kwargs or {}),
            }
        )
        call_api(
            description=f"Update job in farm {self.farm.id} and queue {self.queue.id} with kwargs {kwargs}",
            fn=lambda: client.update_job(
                farmId=self.farm.id,
                queueId=self.queue.id,
                jobId=self.id,
                **kwargs,
            ),
        )

    def wait_until_complete(
        self,
        *,
        client: DeadlineClient,
        wait_interval_sec: int = 10,
        max_retries: Optional[int] = 20,
    ) -> None:
        """
        Waits until the job is complete.
        This method will refresh the job info until the job is complete or the operation times out.

        Args:
            wait_interval_sec (int, optional): Interval between waits in seconds. Defaults to 5.
            max_retries (int, optional): Maximum retry count. Defaults to 20.
        """

        def _is_job_complete():
            self.refresh_job_info(client=client)
            if not self.complete:
                LOG.info(f"Job {self.id} not complete")
            return self.complete

        wait_for(
            description=f"job {self.id} to complete",
            predicate=_is_job_complete,
            interval_s=wait_interval_sec,
            max_retries=max_retries,
        )

    def list_steps(
        self,
        *,
        deadline_client: DeadlineClient,
    ) -> Generator[Step, None, None]:
        list_steps_paginator: Paginator = deadline_client.get_paginator("list_steps")
        list_steps_pages: PageIterator = call_api(
            description=f"Listing steps for job {self.id}",
            fn=lambda: list_steps_paginator.paginate(
                farmId=self.farm.id,
                queueId=self.queue.id,
                jobId=self.id,
            ),
        )

        for page in list_steps_pages:
            for step in page["steps"]:
                dependency_counts = step.get("dependencyCounts", None)
                yield Step(
                    farm=self.farm,
                    queue=self.queue,
                    job=self,
                    id=step["stepId"],
                    name=step["name"],
                    created_at=step["createdAt"],
                    created_by=step["createdBy"],
                    lifecycle_status=StepLifecycleStatus(step["lifecycleStatus"]),
                    task_run_status=TaskStatus(step["taskRunStatus"]),
                    task_run_status_counts={
                        TaskStatus(key): value for key, value in step["taskRunStatusCounts"].items()
                    },
                    lifecycle_status_message=step.get("lifeCycleStatusMessage", None),
                    target_task_run_status=step.get("targetTaskRunStatus", None),
                    updated_at=step.get("updatedAt", None),
                    updated_by=step.get("updatedBy", None),
                    started_at=step.get("startedAt", None),
                    ended_at=step.get("endedAt", None),
                    dependency_counts=(
                        DependencyCounts.from_api_response(dependency_counts)
                        if dependency_counts is not None
                        else None
                    ),
                )

    def get_only_task(
        self,
        *,
        deadline_client: DeadlineClient,
    ) -> Task:
        """
        Asserts that the job has a single step and a single task, and returns the task.

        Args:
            deadline_client (deadline_test_fixtures.client.DeadlineClient): Deadline boto client
        Return:
            task: The single task of the job
        """
        # Assert there is a single step and task
        steps = list(self.list_steps(deadline_client=deadline_client))
        assert len(steps) == 1, "Job contains multiple steps"
        step = steps[0]
        tasks = list(step.list_tasks(deadline_client=deadline_client))
        assert len(tasks) == 1, "Job contains multiple tasks"
        return tasks[0]

    def assert_single_task_log_contains(
        self,
        *,
        deadline_client: DeadlineClient,
        logs_client: BaseClient,
        expected_pattern: re.Pattern | str,
        assert_fail_msg: str = "Expected message not found in session log",
        retries: int = 6,
        backoff_factor: timedelta = timedelta(milliseconds=300),
    ) -> None:
        """
        Asserts that the expected regular expression pattern exists in the job's session log.

        This method is intended for jobs with a single step and task. It checks the logs of the
        last run session for the single task.

        The method accounts for the eventual-consistency of CloudWatch log delivery and availability
        through CloudWatch APIs by retrying a configurable number of times using retries and
        backs-off exponentially if the pattern is not initially found for a configurable number of
        times.

        Args:
            deadline_client (deadline_test_fixtures.client.DeadlineClient): Deadline boto client
            logs_client (botocore.clients.BaseClient): CloudWatch logs boto client
            expected_pattern (re.Pattern | str): Either a regular expression pattern string, or a
                pre-compiled regular expression Pattern object. This is pattern is searched against
                each of the job's session logs, contatenated as a multi-line string joined by
                a single newline character (\\n).
            assert_fail_msg (str): The assertion message to raise if the pattern is not found after
                the configured exponential backoff attempts. The CloudWatch log group name is
                appended to the end of this message to assist with diagnosis. The default is
                "Expected message not found in session log".
            retries (int): The number of retries with exponential back-off to attempt while the
                expected pattern is not found. Default is 4.
            backoff_factor (datetime.timedelta): A multiple used for exponential back-off delay
                between attempts when the expected pattern is not found. The formula used is:

                delay = backoff_factor * 2 ** i

                where i is the 0-based retry number

                Default is 300ms
        """
        # Coerce Regex str patterns to a re.Pattern
        if isinstance(expected_pattern, str):
            expected_pattern = re.compile(expected_pattern)

        task = self.get_only_task(deadline_client=deadline_client)

        session = task.get_last_session(deadline_client=deadline_client)
        session.assert_log_contains(
            logs_client=logs_client,
            expected_pattern=expected_pattern,
            assert_fail_msg=assert_fail_msg,
            backoff_factor=backoff_factor,
            retries=retries,
        )

    def assert_single_task_log_does_not_contain(
        self,
        *,
        deadline_client: DeadlineClient,
        logs_client: BaseClient,
        expected_pattern: re.Pattern | str,
        assert_fail_msg: str = "Expected message found in session log",
        consistency_wait_time: timedelta = timedelta(seconds=3),
    ) -> None:
        """
        Asserts that the expected regular expression pattern doesn't exist in the job's session log.

        This method is intended for jobs with a single step and task. It checks the logs of the
        last run session for the single task.

        The method accounts for the eventual-consistency of CloudWatch log delivery and availability
        through CloudWatch APIs by a configurable wait time. The method does an initial immediate
        check, then waits for the configured consistency wait time before checking again, if the
        wait time is greater than zero. If neither check (or one check if wait time is zero) matches
        the expected pattern then the log is assumed to not contain the given line.

        Args:
            deadline_client (deadline_test_fixtures.client.DeadlineClient): Deadline boto client
            logs_client (botocore.clients.BaseClient): CloudWatch logs boto client
            expected_pattern (re.Pattern | str): Either a regular expression pattern string, or a
                pre-compiled regular expression Pattern object. This is pattern is searched against
                each of the job's session logs, contatenated as a multi-line string joined by
                a single newline character (\\n).
            assert_fail_msg (str): The assertion message to raise if the pattern is found
                The CloudWatch log group name is appended to the end of this message to assist
                with diagnosis. The default is "Expected message found in session log".
            consistency_wait_time (datetime.timedelta): Wait time between first and second check.
                Default is 3s, wait times opperates in second increments.
        """
        # Coerce Regex str patterns to a re.Pattern
        if isinstance(expected_pattern, str):
            expected_pattern = re.compile(expected_pattern)

        task = self.get_only_task(deadline_client=deadline_client)

        session = task.get_last_session(deadline_client=deadline_client)
        session.assert_log_does_not_contain(
            logs_client=logs_client,
            expected_pattern=expected_pattern,
            assert_fail_msg=assert_fail_msg,
            consistency_wait_time=consistency_wait_time,
        )

    @property
    def complete(self) -> bool:  # pragma: no cover
        return self.task_run_status in COMPLETE_TASK_STATUSES

    def __str__(self) -> str:  # pragma: no cover
        if self.task_run_status_counts:
            task_run_status_counts = "\n".join(
                [
                    f"\t{k}: {v}"
                    for k, v in sorted(
                        filter(lambda i: i[1] > 0, self.task_run_status_counts.items()),
                        key=lambda i: i[1],
                        reverse=True,
                    )
                ]
            )
        else:
            task_run_status_counts = str(self.task_run_status_counts)

        return "\n".join(
            [
                "Job:",
                f"id: {self.id}",
                f"name: {self.name}",
                f"description: {self.description}",
                f"farm: {self.farm.id}",
                f"queue: {self.queue.id}",
                f"template: {json.dumps(self.template)}",
                f"parameters: {self.parameters}",
                f"attachments: {self.attachments}",
                f"lifecycle_status: {self.lifecycle_status}",
                f"lifecycle_status_message: {self.lifecycle_status_message}",
                f"priority: {self.priority}",
                f"target_task_run_status: {self.target_task_run_status}",
                f"task_run_status: {self.task_run_status}",
                f"task_run_status_counts:\n{task_run_status_counts}",
                f"storage_profile_id: {self.storage_profile_id}",
                f"max_failed_tasks_count: {self.max_failed_tasks_count}",
                f"max_retries_per_task: {self.max_retries_per_task}",
                f"created_at: {self.created_at}",
                f"created_by: {self.created_by}",
                f"updated_at: {self.updated_at}",
                f"updated_by: {self.updated_by}",
                f"started_at: {self.started_at}",
                f"ended_at: {self.ended_at}",
            ]
        )


@dataclass
class DependencyCounts:
    consumers_resolved: int
    consumers_unresolved: int
    dependencies_resolved: int
    dependencies_unresolved: int

    @staticmethod
    def from_api_response(response: dict[str, Any]) -> DependencyCounts:
        return DependencyCounts(
            consumers_resolved=response["consumersResolved"],
            consumers_unresolved=response["consumersUnresolved"],
            dependencies_resolved=response["dependenciesResolved"],
            dependencies_unresolved=response["dependenciesUnresolved"],
        )


@dataclass
class Step:
    farm: Farm
    queue: Queue
    job: Job
    id: str

    name: str
    created_at: datetime.datetime
    created_by: str
    lifecycle_status: StepLifecycleStatus
    task_run_status: TaskStatus
    task_run_status_counts: dict[TaskStatus, int]
    lifecycle_status_message: str | None = None
    target_task_run_status: TaskStatus | None = None
    updated_at: datetime.datetime | None = None
    updated_by: str | None = None
    started_at: datetime.datetime | None = None
    ended_at: datetime.datetime | None = None
    dependency_counts: DependencyCounts | None = None

    def list_tasks(
        self,
        *,
        deadline_client: DeadlineClient,
    ) -> Generator[Task, None, None]:
        list_tasks_paginator: Paginator = deadline_client.get_paginator("list_tasks")
        list_tasks_pages: PageIterator = call_api(
            description=f"Listing steps for job {self.job.id}",
            fn=lambda: list_tasks_paginator.paginate(
                farmId=self.farm.id,
                queueId=self.queue.id,
                jobId=self.job.id,
                stepId=self.id,
            ),
        )
        for page in list_tasks_pages:
            for task in page["tasks"]:
                target_task_run_status = task.get("targetTaskRunStatus", None)
                yield Task(
                    farm=self.farm,
                    queue=self.queue,
                    job=self.job,
                    step=self,
                    id=task["taskId"],
                    created_at=task["createdAt"],
                    created_by=task["createdBy"],
                    run_status=task["runStatus"],
                    failure_retry_count=task["failureRetryCount"],
                    latest_session_action_id=task.get("latestSessionActionId", None),
                    parameters=task.get("parameters", None),
                    target_task_run_status=(
                        TaskStatus(target_task_run_status) if target_task_run_status else None
                    ),
                    updated_at=task.get("updatedAt", None),
                    updated_by=task.get("updatedBy", None),
                    started_at=task.get("startedAt", None),
                    ended_at=task.get("endedAt", None),
                )


@dataclass
class FloatTaskParameterValue:
    float: str


@dataclass
class IntTaskParameterValue:
    int: str


@dataclass
class PathTaskParameterValue:
    path: str


@dataclass
class StringTaskParameterValue:
    string: str


TaskParameterValue = Union[
    FloatTaskParameterValue, IntTaskParameterValue, PathTaskParameterValue, StringTaskParameterValue
]


@dataclass
class Task:
    farm: Farm
    queue: Queue
    job: Job
    step: Step
    id: str

    created_at: datetime.datetime
    created_by: str
    run_status: TaskStatus
    ended_at: datetime.datetime | None = None
    failure_retry_count: int | None = None
    latest_session_action_id: str | None = None
    parameters: dict[str, TaskParameterValue] | None = None
    started_at: datetime.datetime | None = None
    target_task_run_status: TaskStatus | None = None
    updated_at: datetime.datetime | None = None
    updated_by: str | None = None

    def get_last_session(
        self,
        *,
        deadline_client: DeadlineClient,
    ) -> Session:
        if not self.latest_session_action_id:
            raise ValueError(f"No latest session action ID for {self.id}")
        match = re.search(
            r"^sessionaction-(?P<session_id_hex>[a-f0-9]{32})-\d+$", self.latest_session_action_id
        )
        if not match:
            raise ValueError(
                f"Latest session action ID for task {self.id} ({self.latest_session_action_id}) does not match the expected pattern."
            )
        session_id_hex = match.group("session_id_hex")
        session_id = f"session-{session_id_hex}"
        session = deadline_client.get_session(
            farmId=self.farm.id,
            queueId=self.queue.id,
            jobId=self.job.id,
            sessionId=session_id,
        )
        host_properties = session.get("hostProperties", None)
        return Session(
            farm=self.farm,
            queue=self.queue,
            job=self.job,
            fleet=Fleet(session["fleetId"], farm=self.farm),
            id=session["sessionId"],
            lifecycle_status=session["lifecycleStatus"],
            worker_log=LogConfiguration.from_api_response(session["workerLog"]),
            host_properties=(
                WorkerHostProperties.from_api_response(host_properties) if host_properties else None
            ),
            logs=LogConfiguration.from_api_response(session["log"]),
            started_at=session.get("startedAt", None),
            ended_at=session.get("endedAt", None),
            target_lifecycle_status=session.get("targetLifecycleStatus", None),
            updated_at=session.get("updatedAt", None),
            updated_by=session.get("updatedBy", None),
            worker_id=session["workerId"],
        )

    def list_sessions(self, *, deadline_client: DeadlineClient) -> Generator[Session, None, None]:
        list_sessions_paginator: Paginator = deadline_client.get_paginator("list_sessions")
        list_sessions_pages: PageIterator = call_api(
            description=f"Listing steps for job {self.job.id}",
            fn=lambda: list_sessions_paginator.paginate(
                farmId=self.farm.id,
                queueId=self.queue.id,
                jobId=self.id,
            ),
        )
        for page in list_sessions_pages:
            for session in page["sessions"]:
                host_properties = session.get("hostProperties", None)
                worker_log_config = session.get("workerLog", None)
                yield Session(
                    farm=self.farm,
                    queue=self.queue,
                    job=self.job,
                    fleet=Fleet(session["fleetId"], farm=self.farm),
                    id=session["sessionId"],
                    lifecycle_status=session["lifecycleStatus"],
                    host_properties=(
                        WorkerHostProperties.from_api_response(host_properties)
                        if host_properties
                        else None
                    ),
                    logs=LogConfiguration.from_api_response(session["log"]),
                    started_at=session.get("startedAt", None),
                    ended_at=session.get("endedAt", None),
                    target_lifecycle_status=session.get("targetLifecycleStatus", None),
                    updated_at=session.get("updatedAt", None),
                    updated_by=session.get("updatedBy", None),
                    worker_id=session["workerId"],
                    worker_log=(
                        LogConfiguration.from_api_response(worker_log_config)
                        if worker_log_config
                        else None
                    ),
                )


@dataclass
class JobLogs:
    log_group_name: str
    logs: dict[str, list[CloudWatchLogEvent]]

    @property
    def session_logs(self) -> dict[str, SessionLog]:
        return {
            session_id: SessionLog(session_id=session_id, logs=logs)
            for session_id, logs in self.logs.items()
        }


@dataclass
class LogConfiguration:
    log_driver: Literal["awslogs"]
    error: str | None = None
    options: dict[str, str] | None = None
    parameters: dict[str, str] | None = None

    @staticmethod
    def from_api_response(response: dict[str, Any]) -> LogConfiguration:
        return LogConfiguration(
            log_driver=response["logDriver"],
            error=response.get("error", None),
            options=response.get("options", None),
            parameters=response.get("parameters", None),
        )


@dataclass
class Session:
    farm: Farm
    queue: Queue
    job: Job
    fleet: Fleet
    id: str

    lifecycle_status: SessionLifecycleStatus
    logs: LogConfiguration
    started_at: datetime.datetime
    worker_id: str
    ended_at: datetime.datetime | None = None
    host_properties: WorkerHostProperties | None = None
    target_lifecycle_status: Literal["ENDED"] | None = None
    updated_at: datetime.datetime | None = None
    updated_by: str | None = None
    worker_log: LogConfiguration | None = None

    def get_session_log(self, *, logs_client: BaseClient) -> SessionLog:
        if not (log_driver := self.logs.log_driver):
            raise ValueError('No "logDriver" key in session API response')
        elif log_driver != "awslogs":
            raise NotImplementedError(f'Unsupported log driver "{log_driver}"')
        if not (session_log_config_options := self.logs.options):
            raise ValueError('No "options" key in session "log" API response')
        if not (log_group_name := session_log_config_options.get("logGroupName", None)):
            raise ValueError('No "logGroupName" key in session "log" -> "options" API response')
        if not (log_stream_name := session_log_config_options.get("logStreamName", None)):
            raise ValueError('No "logStreamName" key in session "log" -> "options" API response')

        filter_log_events_paginator: Paginator = logs_client.get_paginator("filter_log_events")
        filter_log_events_pages: PageIterator = call_api(
            description=f"Fetching log events for session {self.id} in log group {log_group_name}",
            fn=lambda: filter_log_events_paginator.paginate(
                logGroupName=log_group_name,
                logStreamNames=[log_stream_name],
            ),
        )
        log_events = filter_log_events_pages.build_full_result()
        log_events = [CloudWatchLogEvent.from_api_response(e) for e in log_events["events"]]

        return SessionLog(session_id=self.id, logs=log_events)

    def assert_log_contains(
        self,
        *,
        logs_client: BaseClient,
        expected_pattern: re.Pattern | str,
        assert_fail_msg: str = "Expected message not found in session log",
        retries: int = 6,
        backoff_factor: timedelta = timedelta(milliseconds=300),
    ) -> None:
        """
        Asserts that the expected regular expression pattern exists in the job's session log.

        This method accounts for the eventual-consistency of CloudWatch log delivery and
        availability through CloudWatch APIs by retrying a configurable number of times using
        exponential back-off if the pattern is not initially found.

        Args:
            logs_client (botocore.clients.BaseClient): CloudWatch logs boto client
            expected_pattern (re.Pattern | str): Either a regular expression pattern string, or a
                pre-compiled regular expression Pattern object. This is pattern is searched against
                each of the job's session logs, contatenated as a multi-line string joined by
                a single newline character (\\n).
            assert_fail_msg (str): The assertion message to raise if the pattern is not found after
                the configured exponential backoff attempts. The CloudWatch log group name is
                appended to the end of this message to assist with diagnosis. The default is
                "Expected message not found in session log".
            retries (int): The number of retries with exponential back-off to attempt while the
                expected pattern is not found. Default is 4.
            backoff_factor (datetime.timedelta): A multiple used for exponential back-off delay
                between attempts when the expected pattern is not found. The formula used is:

                delay = backoff_factor * 2 ** i

                where i is the 0-based retry number

                Default is 300ms
        """
        # Coerce Regex str patterns to a re.Pattern
        if isinstance(expected_pattern, str):
            expected_pattern = re.compile(expected_pattern)

        if not (session_log_config_options := self.logs.options):
            raise ValueError('No "options" key in session "log" API response')
        if not (log_group_name := session_log_config_options.get("logGroupName", None)):
            raise ValueError('No "logGroupName" key in session "log" -> "options" API response')

        for i in range(retries + 1):
            session_log = self.get_session_log(logs_client=logs_client)

            try:
                session_log.assert_pattern_in_log(
                    expected_pattern=expected_pattern,
                    failure_msg=f"{assert_fail_msg}. Logs are in CloudWatch log group: {log_group_name}",
                )
            except AssertionError:
                if i == retries:
                    raise
                else:
                    delay: timedelta = (2**i) * backoff_factor
                    LOG.warning(
                        f"Expected pattern not found in session log {self.id}, delaying {delay} then retry."
                    )
                    time.sleep(delay.total_seconds())
            else:
                return

    def assert_log_does_not_contain(
        self,
        *,
        logs_client: BaseClient,
        expected_pattern: re.Pattern | str,
        assert_fail_msg: str = "Expected message found in session log",
        consistency_wait_time: timedelta = timedelta(seconds=4.5),
    ) -> None:
        """
        Asserts that the expected regular expression pattern does not exist in the job's session log.

        This method accounts for the eventual-consistency of CloudWatch log delivery and availability
        through CloudWatch APIs by a configurable wait time. The method does an initial immediate
        check, then waits for the configured consistency wait time before checking again, if the wait
        time is greater than zero. If neither check (or one check if wait time is zero) matches the
        expected pattern then the log is assumed to not contain the given line.

        Args:
            logs_client (botocore.clients.BaseClient): CloudWatch logs boto client
            expected_pattern (re.Pattern | str): Either a regular expression pattern string, or a
                pre-compiled regular expression Pattern object. This is pattern is searched against
                each of the job's session logs, contatenated as a multi-line string joined by
                a single newline character (\\n).
            assert_fail_msg (str): The assertion message to raise if the pattern is found
                The CloudWatch log group name is appended to the end of this message to assist
                with diagnosis. The default is "Expected message found in session log".
            consistency_wait_time (datetime.timedelta): Wait time between first and second check.
                Default is 4.5 seconds.
        """
        # Coerce Regex str patterns to a re.Pattern
        if isinstance(expected_pattern, str):
            expected_pattern = re.compile(expected_pattern)

        if not (session_log_config_options := self.logs.options):
            raise ValueError('No "options" key in session "log" API response')
        if not (log_group_name := session_log_config_options.get("logGroupName", None)):
            raise ValueError('No "logGroupName" key in session "log" -> "options" API response')

        session_log = self.get_session_log(logs_client=logs_client)
        session_log.assert_pattern_not_in_log(
            expected_pattern=expected_pattern,
            failure_msg=f"{assert_fail_msg}. Logs are in CloudWatch log group: {log_group_name}",
        )
        if consistency_wait_time.total_seconds() > 0:
            time.sleep(consistency_wait_time.total_seconds())
            session_log = self.get_session_log(logs_client=logs_client)
            session_log.assert_pattern_not_in_log(
                expected_pattern=expected_pattern,
                failure_msg=f"{assert_fail_msg}. Logs are in CloudWatch log group: {log_group_name}",
            )
        else:
            LOG.warning(
                "Expected pattern only checked for once. To check twice use non-zero consistency_wait_time"
            )


@dataclass
class CloudWatchLogs:
    logs: list[CloudWatchLogEvent]

    def assert_pattern_in_log(
        self,
        *,
        expected_pattern: re.Pattern | str,
        failure_msg: str,
    ) -> None:
        """
        Asserts that a pattern is found in the session log

        Args:
            expected_pattern (re.Pattern | str): Either a regular expression pattern string, or a
                pre-compiled regular expression Pattern object. This is pattern is searched against
                each of the job's session logs, contatenated as a multi-line string joined by
                a single newline character (\\n).
            failure_msg (str): A message to be raised in an AssertionError if the expected pattern
                is not found

        Raises:
            AssertionError
                Raised when the expected pattern is not found in the session log. The argument to
                the AssertionError is the value of the failure_msg argument
        """
        # Coerce Regex str patterns to a re.Pattern
        if isinstance(expected_pattern, str):
            expected_pattern = re.compile(expected_pattern)

        full_session_log = "\n".join(le.message for le in self.logs)
        assert expected_pattern.search(full_session_log), failure_msg

    def assert_pattern_not_in_log(
        self,
        *,
        expected_pattern: re.Pattern | str,
        failure_msg: str,
    ) -> None:
        """
        Asserts that a pattern is not found in the session log

        Args:
            expected_pattern (re.Pattern | str): Either a regular expression pattern string, or a
                pre-compiled regular expression Pattern object. This is pattern is searched against
                each of the job's session logs, contatenated as a multi-line string joined by
                a single newline character (\\n).
            failure_msg (str): A message to be raised in an AssertionError if the expected pattern
                is found.

        Raises:
            AssertionError
                Raised when the expected pattern is found in the session log. The argument to
                the AssertionError is the value of the failure_msg argument
        """
        # Coerce Regex str patterns to a re.Pattern
        if isinstance(expected_pattern, str):
            expected_pattern = re.compile(expected_pattern)

        full_session_log = "\n".join(le.message for le in self.logs)

        assert True if expected_pattern.search(full_session_log) is None else False, failure_msg


@dataclass
class CloudWatchLogEvent:
    ingestion_time: int
    message: str
    timestamp: int

    @staticmethod
    def from_api_response(response: dict) -> CloudWatchLogEvent:
        return CloudWatchLogEvent(
            ingestion_time=response["ingestionTime"],
            message=response["message"],
            timestamp=response["timestamp"],
        )


@dataclass
class SessionLog(CloudWatchLogs):
    session_id: str


@dataclass
class WorkerLog(CloudWatchLogs):
    worker_id: str
