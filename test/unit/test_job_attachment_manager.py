# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from datetime import datetime
from unittest import mock

import pytest
from botocore.exceptions import ClientError, WaiterError
from botocore.stub import ANY, Stubber

from deadline_test_scaffolding import JobAttachmentManager


class TestJobAttachmentManager:
    """
    Test suite for the job attachment manager
    """

    @pytest.fixture(autouse=True)
    def setup_test(self, mock_get_deadline_models, boto_config):
        with mock.patch("deadline_test_scaffolding.job_attachment_manager.DeadlineManager"):
            self.job_attachment_manager = JobAttachmentManager(
                stage="test", account_id="123456789101"
            )
            yield

    def test_deploy_resources(self):
        """
        Test that during the normal flow that the upgrade stack boto call is made.
        """
        # WHEN
        with Stubber(self.job_attachment_manager.stack.meta.client) as stubber:
            stubber.add_response(
                "update_stack",
                {
                    "StackId": "arn:aws:cloudformation:us-west-2:123456789101:stack/"
                    "JobAttachmentIntegTest/abcdefgh-1234-ijkl-5678-mnopqrstuvwx"
                },
                expected_params={
                    "StackName": "JobAttachmentIntegTest",
                    "TemplateBody": ANY,
                    "Parameters": [
                        {
                            "ParameterKey": "BucketName",
                            "ParameterValue": "job-attachment-integ-test-test-123456789101",
                        },
                    ],
                },
            )
            stubber.add_response(
                "describe_stacks",
                {
                    "Stacks": [
                        {
                            "StackName": "JobAttachmentIntegTest",
                            "CreationTime": datetime(2015, 1, 1),
                            "StackStatus": "UPDATE_COMPLETE",
                        },
                    ],
                },
                expected_params={
                    "StackName": "JobAttachmentIntegTest",
                },
            )
            self.job_attachment_manager.deploy_resources()

            stubber.assert_no_pending_responses()

    @mock.patch(
        "deadline_test_scaffolding.job_attachment_manager." "JobAttachmentManager.cleanup_resources"
    )
    def test_deploy_resources_client_error(
        self,
        mocked_cleanup_resources: mock.MagicMock,
    ):
        """
        Test that if there's an issue deploying resources, the rest get cleaned up.
        """
        # WHEN
        with mock.patch.object(
            self.job_attachment_manager.deadline_manager,
            "create_kms_key",
            side_effect=ClientError(
                {"ErrorCode": "Oops", "Message": "Something went wrong"}, "create_kms_key"
            ),
        ), pytest.raises(ClientError):
            self.job_attachment_manager.deploy_resources()

        mocked_cleanup_resources.assert_called_once()

    @mock.patch(
        "deadline_test_scaffolding.job_attachment_manager." "JobAttachmentManager.cleanup_resources"
    )
    def test_deploy_resources_waiter_error(
        self,
        mocked_cleanup_resources: mock.MagicMock,
    ):
        """
        Test that if there's an issue deploying resources, the rest get cleaned up.
        But this time with a waiter error.
        """
        # WHEN
        with Stubber(self.job_attachment_manager.stack.meta.client) as stubber, pytest.raises(
            WaiterError
        ):
            stubber.add_response(
                "update_stack",
                {
                    "StackId": "arn:aws:cloudformation:us-west-2:123456789101:stack/"
                    "JobAttachmentIntegTest/abcdefgh-1234-ijkl-5678-mnopqrstuvwx"
                },
                expected_params={
                    "StackName": "JobAttachmentIntegTest",
                    "TemplateBody": ANY,
                    "Parameters": [
                        {
                            "ParameterKey": "BucketName",
                            "ParameterValue": "job-attachment-integ-test-test-123456789101",
                        },
                    ],
                },
            )
            stubber.add_client_error(
                "describe_stacks", service_error_code="400", service_message="Oops"
            )

            self.job_attachment_manager.deploy_resources()

            stubber.assert_no_pending_responses()

        mocked_cleanup_resources.assert_called_once()

    def test_deploy_stack_update_while_create_in_progress(self):
        """
        Test that if an attempt to update a stack when a stack is in the process of being created,
        we wait for the stack to complete being created.
        """
        # WHEN
        with Stubber(self.job_attachment_manager.stack.meta.client) as stubber:
            stubber.add_client_error(
                "update_stack",
                service_error_code="400",
                service_message="JobAttachmentIntegTest is in CREATE_IN_PROGRESS "
                "state and can not be updated.",
            )
            stubber.add_response(
                "describe_stacks",
                {
                    "Stacks": [
                        {
                            "StackName": "JobAttachmentIntegTest",
                            "CreationTime": datetime(2015, 1, 1),
                            "StackStatus": "CREATE_COMPLETE",
                        },
                    ],
                },
                expected_params={
                    "StackName": "JobAttachmentIntegTest",
                },
            )

            self.job_attachment_manager.deploy_stack()

            stubber.assert_no_pending_responses()

    @mock.patch(
        "deadline_test_scaffolding.job_attachment_manager." "JobAttachmentManager._create_stack"
    )
    def test_deploy_stack_update_while_stack_doesnt_need_updating(
        self, mocked__create_stack: mock.MagicMock
    ):
        """
        Test that if a stack already exists that doesn't need updating, nothing happens.
        """
        # WHEN
        with Stubber(self.job_attachment_manager.stack.meta.client) as stubber:
            stubber.add_client_error(
                "update_stack",
                service_error_code="400",
                service_message="No updates are to be performed.",
            )

            self.job_attachment_manager.deploy_stack()

            stubber.assert_no_pending_responses()

        mocked__create_stack.assert_not_called()

    def test_deploy_stack_stack_doesnt_exist(self):
        """
        Test that if when updating the stack, that it gets created if it doesn't exist.
        """
        # WHEN
        with Stubber(self.job_attachment_manager.stack.meta.client) as stubber:
            stubber.add_client_error(
                "update_stack",
                service_error_code="400",
                service_message="The Stack JobAttachmentIntegTest doesn't exist",
            )
            stubber.add_response(
                "create_stack",
                {
                    "StackId": "arn:aws:cloudformation:us-west-2:123456789101:stack/"
                    "JobAttachmentIntegTest/abcdefgh-1234-ijkl-5678-mnopqrstuvwx"
                },
                expected_params={
                    "StackName": "JobAttachmentIntegTest",
                    "TemplateBody": ANY,
                    "OnFailure": "DELETE",
                    "EnableTerminationProtection": False,
                    "Parameters": [
                        {
                            "ParameterKey": "BucketName",
                            "ParameterValue": "job-attachment-integ-test-test-123456789101",
                        },
                    ],
                },
            )
            stubber.add_response(
                "describe_stacks",
                {
                    "Stacks": [
                        {
                            "StackName": "JobAttachmentIntegTest",
                            "CreationTime": datetime(2015, 1, 1),
                            "StackStatus": "CREATE_COMPLETE",
                        },
                    ],
                },
                expected_params={
                    "StackName": "JobAttachmentIntegTest",
                },
            )

            self.job_attachment_manager.deploy_stack()

            stubber.assert_no_pending_responses()

    def test_deploy_stack_stack_already_exists(self):
        """
        Test the if we try to create a stack when it already exists,
        we wait for it to finish being created.
        """
        # WHEN
        with Stubber(self.job_attachment_manager.stack.meta.client) as stubber:
            stubber.add_client_error(
                "update_stack",
                service_error_code="400",
                service_message="The Stack JobAttachmentIntegTest doesn't exist",
            )
            stubber.add_client_error(
                "create_stack",
                service_error_code="400",
                service_message="Stack [JobAttachmentIntegTest] already exists",
            )
            stubber.add_response(
                "describe_stacks",
                {
                    "Stacks": [
                        {
                            "StackName": "JobAttachmentIntegTest",
                            "CreationTime": datetime(2015, 1, 1),
                            "StackStatus": "CREATE_COMPLETE",
                        },
                    ],
                },
                expected_params={
                    "StackName": "JobAttachmentIntegTest",
                },
            )

            self.job_attachment_manager.deploy_stack()

            stubber.assert_no_pending_responses()

    def test_deploy_stack_other_client_error(self):
        """
        Test that when we create a stack, unhandled client errors get raised.
        """
        # WHEN
        with Stubber(self.job_attachment_manager.stack.meta.client) as stubber, pytest.raises(
            ClientError
        ):
            stubber.add_client_error(
                "update_stack",
                service_error_code="400",
                service_message="The Stack JobAttachmentIntegTest doesn't exist",
            )
            stubber.add_client_error(
                "create_stack",
                service_error_code="400",
                service_message="Oops",
            )

            self.job_attachment_manager.deploy_stack()

            stubber.assert_no_pending_responses()

    def test_empty_bucket_bucket_doesnt_exist(self):
        """
        If we try to empty a bucket that doesn't exist, make sure nothing happens.
        """
        # WHEN
        with Stubber(self.job_attachment_manager.bucket.meta.client) as stubber:
            stubber.add_client_error(
                "list_objects",
                service_error_code="400",
                service_message="The specified bucket does not exist",
            )

            self.job_attachment_manager.empty_bucket()

            stubber.assert_no_pending_responses()

    def test_empty_bucket_any_other_error(self):
        """
        Test that unhandled client errors during bucket creation are raised.
        """
        # WHEN
        with Stubber(self.job_attachment_manager.bucket.meta.client) as stubber, pytest.raises(
            ClientError
        ):
            stubber.add_client_error(
                "list_objects",
                service_error_code="400",
                service_message="Ooops",
            )

            self.job_attachment_manager.empty_bucket()

            stubber.assert_no_pending_responses()

    def test_cleanup_resources(self):
        """
        Test that all resources get cleaned up when they exist.
        """
        self.job_attachment_manager.deadline_manager.farm_id = "farm-asdf"
        self.job_attachment_manager.deadline_manager.kms_key_metadata = {"key_id": "aasdfkj"}
        self.job_attachment_manager.deadline_manager.queue_id = "queue-asdfji"

        # WHEN
        with Stubber(self.job_attachment_manager.bucket.meta.client) as stubber:
            stubber.add_response(
                "list_objects",
                {
                    "Contents": [],
                },
            )
            self.job_attachment_manager.cleanup_resources()

            stubber.assert_no_pending_responses()

        self.job_attachment_manager.deadline_manager.delete_farm.assert_called_once()  # type: ignore[attr-defined] # noqa
        self.job_attachment_manager.deadline_manager.delete_kms_key.assert_called_once()  # type: ignore[attr-defined] # noqa
        self.job_attachment_manager.deadline_manager.delete_queue.assert_called_once()  # type: ignore[attr-defined] # noqa

    def test_cleanup_resources_no_resource_exist(self):
        """
        Test that no deletion calls are made when resources don't exist.
        """
        # WHEN
        with Stubber(self.job_attachment_manager.bucket.meta.client) as stubber:
            stubber.add_response(
                "list_objects",
                {
                    "Contents": [],
                },
            )
            self.job_attachment_manager.cleanup_resources()

            stubber.assert_no_pending_responses()

        self.job_attachment_manager.deadline_manager.create_farm.assert_not_called()  # type: ignore[attr-defined] # noqa
        self.job_attachment_manager.deadline_manager.create_kms_key.assert_not_called()  # type: ignore[attr-defined] # noqa
        self.job_attachment_manager.deadline_manager.create_queue.assert_not_called()  # type: ignore[attr-defined] # noqa
