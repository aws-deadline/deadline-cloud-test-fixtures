# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Generator
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError, WaiterError
from moto import mock_aws

from deadline_test_fixtures import job_attachment_manager as jam_module
from deadline_test_fixtures import DeadlineClient, JobAttachmentManager


class TestJobAttachmentManager:
    """
    Test suite for the job attachment manager
    """

    @pytest.fixture(autouse=True)
    def mock_queue_cls(self) -> Generator[MagicMock, None, None]:
        with patch.object(jam_module, "Queue") as mock:
            yield mock

    @pytest.fixture
    def job_attachment_manager(
        self,
    ) -> Generator[JobAttachmentManager, None, None]:
        with mock_aws():
            yield JobAttachmentManager(
                s3_client=boto3.client("s3"),
                deadline_client=DeadlineClient(MagicMock()),
                stage="test",
                account_id="123456789101",
                farm_id="farm-123450981092384",
                bucket_name="job-attachment-bucket-name",
            )

    class TestDeployResources:
        def test_deploys_all_resources(
            self,
            job_attachment_manager: JobAttachmentManager,
            mock_queue_cls: MagicMock,
        ):
            """
            Tests that all resources are created when deploy_resources is called
            """
            # WHEN
            job_attachment_manager.deploy_resources()

            # THEN
            mock_queue_cls.create.call_count == 2

        @pytest.mark.parametrize(
            "error",
            [
                ClientError({}, None),
                WaiterError(None, None, None),
            ],
        )
        def test_cleans_up_when_error_is_raised(
            self,
            error: Exception,
            job_attachment_manager: JobAttachmentManager,
            mock_queue_cls: MagicMock,
        ):
            """
            Test that if there's an issue deploying resources, the rest get cleaned up.
            """
            # GIVEN
            possible_failures: list[MagicMock] = [
                mock_queue_cls.create,
            ]
            for possible_failure in possible_failures:
                possible_failure.side_effect = error

                with (
                    patch.object(
                        job_attachment_manager,
                        "cleanup_resources",
                        wraps=job_attachment_manager.cleanup_resources,
                    ) as spy_cleanup_resources,
                    pytest.raises(type(error)) as raised_exc,
                ):
                    # WHEN
                    job_attachment_manager.deploy_resources()

                # THEN
                assert raised_exc.value is error
                spy_cleanup_resources.assert_called_once()

    class TestEmptyBucket:
        def test_deletes_all_objects_under_prefix(
            self, job_attachment_manager: JobAttachmentManager
        ):
            # GIVEN
            bucket = boto3.resource("s3").Bucket(job_attachment_manager.bucket_name)
            bucket.create()
            bucket.put_object(
                Key=job_attachment_manager.bucket_root_prefix + "/" + "test-object",
                Body="Hello world".encode(),
            )
            bucket.put_object(
                Key=job_attachment_manager.bucket_root_prefix + "/" + "test-object-2",
                Body="Hello world 2".encode(),
            )
            bucket.put_object(
                Key="differen-prefix" + "/" + "test-object-2", Body="Hello world 2".encode()
            )
            assert len(list(bucket.objects.all())) == 3

            # WHEN
            job_attachment_manager.empty_bucket_under_root_prefix()

            # THEN
            assert len(list(bucket.objects.all())) == 1

        def test_swallows_bucket_doesnt_exist_error(
            self, job_attachment_manager: JobAttachmentManager
        ):
            """
            If we try to empty a bucket that doesn't exist, make sure nothing happens.
            """
            # GIVEN
            # The bucket does not exist (we do not create it)

            try:
                # WHEN
                job_attachment_manager.empty_bucket_under_root_prefix()
            except ClientError as e:
                pytest.fail(
                    f"JobAttachmentManager.empty_bucket raised an error when it shouldn't have: {e}"
                )
            else:
                # THEN
                # Success
                pass

        def test_raises_any_other_error(
            self,
            job_attachment_manager: JobAttachmentManager,
        ):
            """
            Test that unhandled client errors during bucket creation are raised.
            """
            # GIVEN
            exc = ClientError({"Error": {"Message": "test"}}, "test-operation")
            with (
                patch.object(job_attachment_manager, "s3_client") as mock_s3_client,
                pytest.raises(ClientError) as raised_exc,
            ):
                mock_s3_client.list_object_versions.side_effect = exc

                # WHEN
                job_attachment_manager.empty_bucket_under_root_prefix()

            # THEN
            assert raised_exc.value is exc
            mock_s3_client.list_object_versions.assert_called_once()

    def test_cleanup_resources(
        self,
        job_attachment_manager: JobAttachmentManager,
        mock_queue_cls: MagicMock,
    ):
        """
        Test that all resources get cleaned up when they exist.
        """
        # GIVEN
        job_attachment_manager.deploy_resources()

        with patch.object(
            job_attachment_manager,
            "empty_bucket_under_root_prefix",
            wraps=job_attachment_manager.empty_bucket_under_root_prefix,
        ) as spy_empty_bucket:
            # WHEN
            job_attachment_manager.cleanup_resources()

        # THEN
        spy_empty_bucket.assert_called_once()
        mock_queue_cls.create.return_value.delete.call_count == 2
