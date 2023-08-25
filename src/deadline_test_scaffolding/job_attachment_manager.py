# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pathlib

import boto3
from botocore.exceptions import ClientError, WaiterError

from deadline_test_scaffolding.deadline_manager import DeadlineManager


class JobAttachmentManager:
    """
    Responsible for setting up and tearing down job attachment test resources
    """

    RESOURCE_CF_TEMPLATE_LOCATION = pathlib.Path(
        pathlib.Path(__file__).parent / "cf_templates" / "job_attachments.yaml"
    )

    def __init__(self, stage: str, account_id: str):
        cloudformation = boto3.resource("cloudformation")
        s3 = boto3.resource("s3")
        self.stack = cloudformation.Stack("JobAttachmentIntegTest")
        self.deadline_manager = DeadlineManager(should_add_deadline_models=True)
        self.bucket = s3.Bucket(f"job-attachment-integ-test-{stage.lower()}-{account_id}")

    def deploy_resources(self):
        """
        Deploy all of the resources needed for job attachment integration tests.
        """
        try:
            self.deadline_manager.create_kms_key()
            self.deadline_manager.create_farm("job_attachments_test_farm")
            self.deadline_manager.create_queue("job_attachments_test_queue")
            self.deploy_stack()
        except (ClientError, WaiterError):
            # If anything goes wrong, rollback
            self.cleanup_resources()
            raise

    def _create_stack(self, template_body: str):
        try:
            # The stack resource doesn't have an action for creating the stack,
            # only updating it. So we need to go through the client.
            self.stack.meta.client.create_stack(
                StackName=self.stack.name,
                TemplateBody=template_body,
                OnFailure="DELETE",
                EnableTerminationProtection=False,
                Parameters=[
                    {
                        "ParameterKey": "BucketName",
                        "ParameterValue": self.bucket.name,
                    },
                ],
            )
        except ClientError as e:
            # Sometimes the cloudformation create stack waiter will release even if if the stack
            # isn't in create_complete. So we have to catch that here and move on.
            if e.response["Error"]["Message"] != f"Stack [{self.stack.name}] already exists":
                raise

        waiter = self.stack.meta.client.get_waiter("stack_create_complete")
        waiter.wait(
            StackName=self.stack.name,
        )

    def deploy_stack(self):
        """
        Deploy the job attachment test stack to the test account. If the stack already exists then
        update it, if the stack doesn't exist then create it.

        Keep the stack around between tests to reduce further test times.
        """
        with open(self.RESOURCE_CF_TEMPLATE_LOCATION) as f:
            template_body = f.read()

        try:
            self.stack.update(
                TemplateBody=template_body,
                Parameters=[
                    {
                        "ParameterKey": "BucketName",
                        "ParameterValue": self.bucket.name,
                    },
                ],
            )
            waiter = self.stack.meta.client.get_waiter("stack_update_complete")
            waiter.wait(StackName=self.stack.name)
        except ClientError as e:
            if (
                "is in CREATE_IN_PROGRESS state and can not be updated."
                in e.response["Error"]["Message"]
            ):
                waiter = self.stack.meta.client.get_waiter("stack_create_complete")
                waiter.wait(StackName=self.stack.name)

            elif e.response["Error"]["Message"] != "No updates are to be performed.":
                self._create_stack(template_body)

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
        self.deadline_manager.delete_additional_queues()
        if self.deadline_manager.queue_id:
            self.deadline_manager.delete_queue()
        if self.deadline_manager.farm_id:
            self.deadline_manager.delete_farm()
        if self.deadline_manager.kms_key_metadata:
            self.deadline_manager.delete_kms_key()
        self.empty_bucket()
