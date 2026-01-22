# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

from dataclasses import InitVar, dataclass, field
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

    def deploy_resources(self):
        """
        Deploy all of the resources needed for job attachment integration tests.
        """
        try:

            self.queue = Queue.create(
                client=self.deadline_client,
                display_name="job_attachments_test_queue",
                farm=Farm(self.farm_id),
                job_attachments=JobAttachmentSettings(
                    bucket_name=self.bucket_name, root_prefix=self.bucket_root_prefix
                ),
            )
            self.queue_with_no_settings = Queue.create(
                client=self.deadline_client,
                display_name="job_attachments_test_no_settings_queue",
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
