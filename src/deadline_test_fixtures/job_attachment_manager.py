# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import logging
from dataclasses import InitVar, dataclass, field
from datetime import datetime, timedelta, timezone
import os
from botocore.client import BaseClient
from botocore.exceptions import ClientError, WaiterError

from .deadline.client import DeadlineClient
from .deadline import (
    Farm,
    Queue,
)

from .models import (
    JobAttachmentSettings,
)
from uuid import uuid4

LOG = logging.getLogger(__name__)

QUEUE_NAME = "job_attachments_test_queue"
QUEUE_NAME_NO_SETTINGS = "job_attachments_test_no_settings_queue"
QUEUE_NAMES = (QUEUE_NAME, QUEUE_NAME_NO_SETTINGS)

# Only delete stale queues older than this. The window is chosen to (a) avoid
# racing concurrent test runs that share the farm, and (b) leave headroom for
# test suites whose runtime may grow over time. Tune down only when confident
# no other process on the farm could be using a same-named queue.
STALE_QUEUE_MIN_AGE = timedelta(days=1)


@dataclass
class JobAttachmentManager:
    """
    Responsible for setting up and tearing down job attachment test resources
    """

    s3_client: BaseClient
    deadline_client: DeadlineClient

    stage: InitVar[str]
    account_id: InitVar[str]

    bucket_name: str
    farm_id: str

    queue: Queue | None = field(init=False, default=None)
    queue_with_no_settings: Queue | None = field(init=False, default=None)
    bucket_root_prefix: str = os.environ.get("JA_TEST_ROOT_PREFIX", "") + str(
        uuid4()
    )  # Set the bucket root prefix for this test run to an UUID to avoid async test execution race conditions

    def _list_all_queues(self) -> list[dict]:
        """Return every queue in the farm, paginating via nextToken."""
        queues = []
        next_token = None
        while True:
            kwargs = {"farmId": self.farm_id}
            if next_token:
                kwargs["nextToken"] = next_token
            response = self.deadline_client.list_queues(**kwargs)
            queues.extend(response.get("queues", []))
            next_token = response.get("nextToken")
            if not next_token:
                return queues

    def _find_stale_queues(self) -> list[dict]:
        """Return test queues older than STALE_QUEUE_MIN_AGE that are safe to delete.

        Skips queues younger than the cutoff so we don't race with concurrent
        test runs sharing the farm.
        """
        cutoff = datetime.now(timezone.utc) - STALE_QUEUE_MIN_AGE
        stale = []
        for queue in self._list_all_queues():
            if queue["displayName"] not in QUEUE_NAMES:
                continue
            if queue["createdAt"] > cutoff:
                LOG.info(
                    f"Skipping recent queue {queue['displayName']} ({queue['queueId']}) "
                    f"createdAt={queue['createdAt']} — may belong to a concurrent test run"
                )
                continue
            stale.append(queue)
        return stale

    def _delete_queues(self, queues: list[dict]) -> None:
        """Delete the given queues, swallowing per-queue ClientErrors so one
        failure does not block the rest."""
        for queue in queues:
            queue_id = queue["queueId"]
            LOG.info(f"Deleting stale queue: {queue['displayName']} ({queue_id})")
            try:
                self.deadline_client.delete_queue(farmId=self.farm_id, queueId=queue_id)
            except ClientError as e:
                LOG.warning(f"Failed to delete stale queue {queue_id}: {e}")
            except Exception as e:
                LOG.warning(f"Unexpected error deleting stale queue {queue_id}: {e}")

    def _cleanup_stale_queues(self) -> None:
        """Delete pre-existing test queues from previous runs that weren't cleaned
        up (e.g. timeouts/crashes). Prevents ServiceQuotaExceededException."""
        LOG.info(f"Checking for stale test queues in farm {self.farm_id}")
        try:
            stale_queues = self._find_stale_queues()
        except ClientError as e:
            LOG.warning(f"Failed to list queues for stale cleanup: {e}")
            return
        except Exception as e:
            LOG.warning(f"Unexpected error listing queues for stale cleanup: {e}")
            return
        self._delete_queues(stale_queues)

    def deploy_resources(self):
        """
        Deploy all of the resources needed for job attachment integration tests.
        """
        self._cleanup_stale_queues()

        try:
            self.queue = Queue.create(
                client=self.deadline_client,
                display_name=QUEUE_NAME,
                farm=Farm(self.farm_id),
                job_attachments=JobAttachmentSettings(
                    bucket_name=self.bucket_name, root_prefix=self.bucket_root_prefix
                ),
            )
            self.queue_with_no_settings = Queue.create(
                client=self.deadline_client,
                display_name=QUEUE_NAME_NO_SETTINGS,
                farm=Farm(self.farm_id),
            )

        except (ClientError, WaiterError):
            # If anything goes wrong, rollback
            self.cleanup_resources()
            raise

    def empty_bucket_under_root_prefix(self):
        """
        Empty the bucket between session runs
        """
        try:
            # List up all objects and their versions in the bucket
            version_list = self.s3_client.list_object_versions(
                Bucket=self.bucket_name, Prefix=self.bucket_root_prefix
            )
            object_list = version_list.get("Versions", []) + version_list.get("DeleteMarkers", [])
            # Delete all objects and versions
            for obj in object_list:
                self.s3_client.delete_object(
                    Bucket=self.bucket_name, Key=obj["Key"], VersionId=obj.get("VersionId", None)
                )

        except ClientError as e:
            if e.response["Error"]["Message"] != "The specified bucket does not exist":
                raise

    def cleanup_resources(self):
        """
        Cleanup all of the resources that the test used
        """
        self.empty_bucket_under_root_prefix()
        if self.queue:
            self.queue.delete(client=self.deadline_client)
        if self.queue_with_no_settings:
            self.queue_with_no_settings.delete(client=self.deadline_client)
