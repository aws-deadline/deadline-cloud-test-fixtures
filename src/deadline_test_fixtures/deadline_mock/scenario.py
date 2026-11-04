# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Serializable data used to seed the hermetic Deadline service."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from typing import Any

FAKE_ACCOUNT_ID = "123456789012"
DEFAULT_FARM_ID = "farm-0000000000000000000000000000000a"
DEFAULT_QUEUE_ID = "queue-0000000000000000000000000000000b"

_FAKE_PRINCIPAL = f"arn:aws:sts::{FAKE_ACCOUNT_ID}:assumed-role/DeadlineTestFixtures/test-runner"
_FAKE_QUEUE_ROLE = f"arn:aws:iam::{FAKE_ACCOUNT_ID}:role/service-role/DeadlineTestFixturesQueueRole"


def _default_farm() -> dict[str, Any]:
    return {
        "farmId": DEFAULT_FARM_ID,
        "displayName": "TestFarm",
        "description": "",
        "createdAt": "2024-01-01T00:00:00+00:00",
        "createdBy": _FAKE_PRINCIPAL,
        "updatedAt": "2024-01-01T00:00:00+00:00",
        "updatedBy": _FAKE_PRINCIPAL,
        "costScaleFactor": 1.0,
    }


def _default_queue() -> dict[str, Any]:
    return {
        "farmId": DEFAULT_FARM_ID,
        "queueId": DEFAULT_QUEUE_ID,
        "displayName": "TestQueue",
        "status": "SCHEDULING",
        "defaultBudgetAction": "NONE",
        "description": "",
        "createdAt": "2024-01-01T00:00:00+00:00",
        "createdBy": _FAKE_PRINCIPAL,
        "updatedAt": "2024-01-01T00:00:00+00:00",
        "updatedBy": _FAKE_PRINCIPAL,
        "jobAttachmentSettings": {
            "s3BucketName": "deadline-test-fixtures-bucket",
            "rootPrefix": "DeadlineCloud",
        },
        "roleArn": _FAKE_QUEUE_ROLE,
        "schedulingConfiguration": {"priorityFifo": {}},
    }


@dataclass(frozen=True)
class MockDeadlineScenario:
    """Resources returned by :class:`MockDeadlineBackend`.

    The default scenario covers the read operations used while a Deadline
    submitter opens and exports a job bundle, plus ``CreateJob``/``GetJob`` for
    submitting one. Consumers can supply queue environments or storage profiles
    without implementing another HTTP mock.

    The default queue has job attachment settings, so a submission that references
    any files would hash and upload them to S3, which the mock does not serve. Use
    :meth:`without_job_attachments` for tests that press Submit.

    ``validate_job_templates`` makes ``CreateJob`` reject what the service would:
    a template that is not a valid Open Job Description job template, or parameter
    values that the template and the queue's environment templates do not accept.
    """

    farm: Mapping[str, Any] = field(default_factory=_default_farm)
    queue: Mapping[str, Any] = field(default_factory=_default_queue)
    queue_environments: tuple[Mapping[str, Any], ...] = ()
    storage_profiles: tuple[Mapping[str, Any], ...] = ()
    response_delay_s: float = 0.0
    validate_job_templates: bool = True

    @classmethod
    def without_job_attachments(cls, **kwargs: Any) -> MockDeadlineScenario:
        """Return a scenario whose queue has no job attachment settings.

        The Deadline client only hashes and uploads job attachments when the queue
        has ``jobAttachmentSettings``, so submitting to this queue goes straight to
        ``CreateJob`` with no S3 traffic. Other fields are passed through; to
        customize the queue as well, pass your own ``queue`` to the constructor and
        leave out ``jobAttachmentSettings``.
        """
        queue = _default_queue()
        del queue["jobAttachmentSettings"]
        return cls(queue=queue, **kwargs)

    @property
    def farm_id(self) -> str:
        return str(self.farm["farmId"])

    @property
    def queue_id(self) -> str:
        return str(self.queue["queueId"])

    def seed(self, backend: Any) -> None:
        """Replace a backend's resources with this scenario."""
        backend.farms = {self.farm_id: deepcopy(dict(self.farm))}
        backend.queues = {
            (self.farm_id, self.queue_id): deepcopy(dict(self.queue)),
        }
        backend.queue_environments = {
            (self.farm_id, self.queue_id): [
                deepcopy(dict(environment)) for environment in self.queue_environments
            ]
        }
        backend.storage_profiles = {
            (self.farm_id, self.queue_id): [
                deepcopy(dict(profile)) for profile in self.storage_profiles
            ]
        }

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> MockDeadlineScenario:
        return cls(
            farm=value["farm"],
            queue=value["queue"],
            queue_environments=tuple(value.get("queue_environments", ())),
            storage_profiles=tuple(value.get("storage_profiles", ())),
            response_delay_s=float(value.get("response_delay_s", 0.0)),
            validate_job_templates=bool(value.get("validate_job_templates", True)),
        )
