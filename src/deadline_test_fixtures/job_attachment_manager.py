# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

from dataclasses import InitVar, dataclass, field

from botocore.client import BaseClient
from botocore.exceptions import ClientError, WaiterError

from .cloudformation import JobAttachmentsBootstrapStack
from .deadline.client import DeadlineClient
from .deadline import (
    Farm,
    Queue,
)

from .models import JobRunAsUser, PosixSessionUser


@dataclass
class JobAttachmentManager:
    """
    Responsible for setting up and tearing down job attachment test resources
    """

    s3_client: BaseClient
    cfn_client: BaseClient
    deadline_client: DeadlineClient

    stage: InitVar[str]
    account_id: InitVar[str]

    stack: JobAttachmentsBootstrapStack = field(init=False)
    farm: Farm | None = field(init=False, default=None)
    queue: Queue | None = field(init=False, default=None)
    queue_with_no_settings: Queue | None = field(init=False, default=None)

    def __post_init__(
        self,
        stage: str,
        account_id: str,
    ):
        self.bucket_name = f"job-attachment-integ-test-{stage.lower()}-{account_id}"
        self.stack = JobAttachmentsBootstrapStack(
            name="JobAttachmentIntegTest",
            bucket_name=self.bucket_name,
        )

    def deploy_resources(self):
        """
        Deploy all of the resources needed for job attachment integration tests.
        """
        try:
            self.farm = Farm.create(
                client=self.deadline_client,
                display_name="job_attachments_test_farm",
            )
            self.queue = Queue.create(
                client=self.deadline_client,
                display_name="job_attachments_test_queue",
                farm=self.farm,
                job_run_as_user=JobRunAsUser(
                    posix=PosixSessionUser("", ""), runAs="WORKER_AGENT_USER"
                ),
            )
            self.queue_with_no_settings = Queue.create(
                client=self.deadline_client,
                display_name="job_attachments_test_no_settings_queue",
                farm=self.farm,
                job_run_as_user=JobRunAsUser(
                    posix=PosixSessionUser("", ""), runAs="WORKER_AGENT_USER"
                ),
            )
            self.stack.deploy(cfn_client=self.cfn_client)
        except (ClientError, WaiterError):
            # If anything goes wrong, rollback
            self.cleanup_resources()
            raise

    def empty_bucket(self):
        """
        Empty the bucket between session runs
        """
        try:
            # List up all objects and their versions in the bucket
            version_list = self.s3_client.list_object_versions(Bucket=self.bucket_name)
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
        Cleanup all of the resources that the test used, except for the stack.
        """
        self.empty_bucket()
        self.stack.destroy(cfn_client=self.cfn_client)
        if self.queue:
            self.queue.delete(client=self.deadline_client)
        if self.queue_with_no_settings:
            self.queue_with_no_settings.delete(client=self.deadline_client)
        if self.farm:
            self.farm.delete(client=self.deadline_client)
