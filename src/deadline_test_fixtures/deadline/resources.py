# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import datetime
import json
import logging
from dataclasses import asdict, dataclass, fields
from enum import Enum
from typing import Any, Callable, Literal, TYPE_CHECKING

from botocore.client import BaseClient

from .client import DeadlineClient
from ..models import JobAttachmentSettings, JobRunAsUser
from ..util import call_api, clean_kwargs, wait_for

if TYPE_CHECKING:
    from botocore.paginate import Paginator, PageIterator

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
        job_run_as_user: JobRunAsUser,
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
                "jobRunAsUser": asdict(job_run_as_user),
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


class TaskStatus(StrEnum):
    UNKNOWN = "UNKNOWN"
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
class Job:
    id: str
    farm: Farm
    queue: Queue
    template: dict

    name: str
    lifecycle_status: str
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
            "lifecycle_status": response["lifecycleStatus"],
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
        max_retries: int | None = None,
    ) -> None:
        """
        Waits until the job is complete.
        This method will refresh the job info until the job is complete or the operation times out.

        Args:
            wait_interval_sec (int, optional): Interval between waits in seconds. Defaults to 5.
            max_retries (int, optional): Maximum retry count. Defaults to None.
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
class JobLogs:
    log_group_name: str
    logs: dict[str, list[CloudWatchLogEvent]]


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
