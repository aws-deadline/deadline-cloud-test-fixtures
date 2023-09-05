# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Generator
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError, WaiterError
from moto import mock_s3

from deadline_test_fixtures import job_attachment_manager as jam_module
from deadline_test_fixtures import DeadlineClient, JobAttachmentManager


class TestJobAttachmentManager:
    """
    Test suite for the job attachment manager
    """

    @pytest.fixture(autouse=True)
    def mock_farm_cls(self) -> Generator[MagicMock, None, None]:
        with patch.object(jam_module, "Farm") as mock:
            yield mock

    @pytest.fixture(autouse=True)
    def mock_queue_cls(self) -> Generator[MagicMock, None, None]:
        with patch.object(jam_module, "Queue") as mock:
            yield mock

    @pytest.fixture(autouse=True)
    def mock_stack(self) -> Generator[MagicMock, None, None]:
        with patch.object(jam_module, "JobAttachmentsBootstrapStack") as mock:
            yield mock.return_value

    @pytest.fixture
    def job_attachment_manager(
        self,
    ) -> Generator[JobAttachmentManager, None, None]:
        with mock_s3():
            yield JobAttachmentManager(
                s3_resource=boto3.resource("s3"),
                cfn_client=MagicMock(),
                deadline_client=DeadlineClient(MagicMock()),
                stage="test",
                account_id="123456789101",
            )

    class TestDeployResources:
        def test_deploys_all_resources(
            self,
            job_attachment_manager: JobAttachmentManager,
            mock_farm_cls: MagicMock,
            mock_queue_cls: MagicMock,
            mock_stack: MagicMock,
        ):
            """
            Tests that all resources are created when deploy_resources is called
            """
            # WHEN
            job_attachment_manager.deploy_resources()

            # THEN
            mock_farm_cls.create.assert_called_once()
            mock_queue_cls.create.assert_called_once()
            mock_stack.deploy.assert_called_once()

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
            mock_farm_cls: MagicMock,
            mock_queue_cls: MagicMock,
            mock_stack: MagicMock,
        ):
            """
            Test that if there's an issue deploying resources, the rest get cleaned up.
            """
            # GIVEN
            possible_failures: list[MagicMock] = [
                mock_farm_cls.create,
                mock_queue_cls.create,
                mock_stack.deploy,
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
        def test_deletes_all_objects(self, job_attachment_manager: JobAttachmentManager):
            # GIVEN
            bucket = job_attachment_manager.bucket
            bucket.create()
            bucket.put_object(Key="test-object", Body="Hello world".encode())
            bucket.put_object(Key="test-object-2", Body="Hello world 2".encode())
            assert len(list(bucket.objects.all())) == 2

            # WHEN
            job_attachment_manager.empty_bucket()

            # THEN
            assert len(list(bucket.objects.all())) == 0

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
                job_attachment_manager.empty_bucket()
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
                patch.object(job_attachment_manager, "bucket") as mock_bucket,
                pytest.raises(ClientError) as raised_exc,
            ):
                mock_bucket.objects.all.side_effect = exc

                # WHEN
                job_attachment_manager.empty_bucket()

            # THEN
            assert raised_exc.value is exc
            mock_bucket.objects.all.assert_called_once()

    def test_cleanup_resources(
        self,
        job_attachment_manager: JobAttachmentManager,
        mock_farm_cls: MagicMock,
        mock_queue_cls: MagicMock,
        mock_stack: MagicMock,
    ):
        """
        Test that all resources get cleaned up when they exist.
        """
        # GIVEN
        job_attachment_manager.deploy_resources()

        with patch.object(
            job_attachment_manager, "empty_bucket", wraps=job_attachment_manager.empty_bucket
        ) as spy_empty_bucket:
            # WHEN
            job_attachment_manager.cleanup_resources()

        # THEN
        spy_empty_bucket.assert_called_once()
        mock_stack.destroy.assert_called_once()
        mock_queue_cls.create.return_value.delete.assert_called_once()
        mock_farm_cls.create.return_value.delete.assert_called_once()
