# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from typing import Any

from botocore.client import BaseClient
from botocore.exceptions import ClientError, WaiterError

from .cloudformation import JobAttachmentsBootstrapStack
from .deadline.client import DeadlineClient
from .deadline import (
    Farm,
    Queue,
)


@dataclass
class JobAttachmentManager:
    """
    Responsible for setting up and tearing down job attachment test resources
    """

    s3_resource: Any
    cfn_client: BaseClient
    deadline_client: DeadlineClient

    stage: InitVar[str]
    account_id: InitVar[str]

    bucket: Any = field(init=False)
    stack: JobAttachmentsBootstrapStack = field(init=False)
    farm: Farm | None = field(init=False, default=None)
    queue: Queue | None = field(init=False, default=None)

    def __post_init__(
        self,
        stage: str,
        account_id: str,
    ):
        self.bucket = self.s3_resource.Bucket(
            f"job-attachment-integ-test-{stage.lower()}-{account_id}"
        )
        self.stack = JobAttachmentsBootstrapStack(
            name="JobAttachmentIntegTest",
            bucket_name=self.bucket.name,
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
            self.bucket.objects.all().delete()
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
        if self.farm:
            self.farm.delete(client=self.deadline_client)
