# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from datetime import datetime, timedelta, timezone
from typing import Generator
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError, WaiterError
from moto import mock_aws

from deadline_test_fixtures import job_attachment_manager as jam_module
from deadline_test_fixtures import DeadlineClient, JobAttachmentManager

OLD = datetime.now(timezone.utc) - timedelta(days=7)
# Ten seconds past the 1-day STALE_QUEUE_MIN_AGE cutoff. Hardcoded (not imported
# from the module) so a regression that loosens the cutoff is caught here. The
# 10-second margin absorbs any drift between this module-level datetime.now()
# at import time and the production code's datetime.now() at test execution.
JUST_STALE = datetime.now(timezone.utc) - timedelta(days=1, seconds=10)
RECENT = datetime.now(timezone.utc) - timedelta(minutes=5)


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
            mock_client = MagicMock()
            mock_client.list_queues.return_value = {"queues": []}
            yield JobAttachmentManager(
                s3_client=boto3.client("s3"),
                deadline_client=DeadlineClient(mock_client),
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
            assert mock_queue_cls.create.call_count == 2

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
        assert mock_queue_cls.create.return_value.delete.call_count == 2

    class TestCleanupStaleQueues:
        def test_deletes_queues_with_matching_names(
            self,
            job_attachment_manager: JobAttachmentManager,
        ):
            """
            Test that stale queues with matching display names are deleted.
            """
            # GIVEN
            mock_client = job_attachment_manager.deadline_client._real_client
            mock_client.list_queues.return_value = {
                "queues": [
                    {
                        "queueId": "queue-stale1",
                        "displayName": "job_attachments_test_queue",
                        "createdAt": OLD,
                    },
                    {
                        "queueId": "queue-stale2",
                        "displayName": "job_attachments_test_no_settings_queue",
                        "createdAt": OLD,
                    },
                    {
                        "queueId": "queue-keep",
                        "displayName": "some_other_queue",
                        "createdAt": OLD,
                    },
                ]
            }

            # WHEN
            job_attachment_manager._cleanup_stale_queues()

            # THEN
            assert mock_client.delete_queue.call_count == 2
            mock_client.delete_queue.assert_any_call(
                farmId=job_attachment_manager.farm_id, queueId="queue-stale1"
            )
            mock_client.delete_queue.assert_any_call(
                farmId=job_attachment_manager.farm_id, queueId="queue-stale2"
            )

        def test_deletes_queue_just_past_cutoff(
            self,
            job_attachment_manager: JobAttachmentManager,
        ):
            """
            A queue created just past STALE_QUEUE_MIN_AGE must be deleted —
            exercises the cutoff lower bound (counterpart to test_skips_recent_queues).
            """
            # GIVEN
            mock_client = job_attachment_manager.deadline_client._real_client
            mock_client.list_queues.return_value = {
                "queues": [
                    {
                        "queueId": "queue-just-stale",
                        "displayName": "job_attachments_test_queue",
                        "createdAt": JUST_STALE,
                    },
                ]
            }

            # WHEN
            job_attachment_manager._cleanup_stale_queues()

            # THEN
            mock_client.delete_queue.assert_called_once_with(
                farmId=job_attachment_manager.farm_id, queueId="queue-just-stale"
            )

        def test_skips_recent_queues(
            self,
            job_attachment_manager: JobAttachmentManager,
        ):
            """
            Recent queues (within STALE_QUEUE_MIN_AGE) must not be deleted — they
            may belong to a concurrently-running test process sharing the farm.
            """
            # GIVEN
            mock_client = job_attachment_manager.deadline_client._real_client
            mock_client.list_queues.return_value = {
                "queues": [
                    {
                        "queueId": "queue-old",
                        "displayName": "job_attachments_test_queue",
                        "createdAt": OLD,
                    },
                    {
                        "queueId": "queue-recent",
                        "displayName": "job_attachments_test_queue",
                        "createdAt": RECENT,
                    },
                ]
            }

            # WHEN
            job_attachment_manager._cleanup_stale_queues()

            # THEN — only the old queue is deleted; recent + missing-timestamp are skipped
            mock_client.delete_queue.assert_called_once_with(
                farmId=job_attachment_manager.farm_id, queueId="queue-old"
            )

        def test_handles_list_queues_error_gracefully(
            self,
            job_attachment_manager: JobAttachmentManager,
        ):
            """
            Test that errors during stale queue cleanup don't block test execution.
            """
            # GIVEN
            mock_client = job_attachment_manager.deadline_client._real_client
            mock_client.list_queues.side_effect = ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "Access denied"}},
                "ListQueues",
            )

            # WHEN / THEN - should not raise
            job_attachment_manager._cleanup_stale_queues()

        def test_paginates_through_all_queues(
            self,
            job_attachment_manager: JobAttachmentManager,
        ):
            """
            Test that stale queue cleanup follows nextToken across multiple pages.
            """
            # GIVEN
            mock_client = job_attachment_manager.deadline_client._real_client
            mock_client.list_queues.side_effect = [
                {
                    "queues": [
                        {
                            "queueId": "queue-page1",
                            "displayName": "job_attachments_test_queue",
                            "createdAt": OLD,
                        },
                    ],
                    "nextToken": "token-1",
                },
                {
                    "queues": [
                        {
                            "queueId": "queue-page2",
                            "displayName": "job_attachments_test_no_settings_queue",
                            "createdAt": OLD,
                        },
                    ],
                },
            ]

            # WHEN
            job_attachment_manager._cleanup_stale_queues()

            # THEN
            assert mock_client.list_queues.call_count == 2
            mock_client.list_queues.assert_any_call(
                farmId=job_attachment_manager.farm_id, nextToken="token-1"
            )
            assert mock_client.delete_queue.call_count == 2
            mock_client.delete_queue.assert_any_call(
                farmId=job_attachment_manager.farm_id, queueId="queue-page1"
            )
            mock_client.delete_queue.assert_any_call(
                farmId=job_attachment_manager.farm_id, queueId="queue-page2"
            )

        def test_handles_delete_queue_error_gracefully(
            self,
            job_attachment_manager: JobAttachmentManager,
        ):
            """
            Test that a failure to delete one stale queue doesn't prevent others from being deleted.
            """
            # GIVEN
            mock_client = job_attachment_manager.deadline_client._real_client
            mock_client.list_queues.return_value = {
                "queues": [
                    {
                        "queueId": "queue-stale1",
                        "displayName": "job_attachments_test_queue",
                        "createdAt": OLD,
                    },
                    {
                        "queueId": "queue-stale2",
                        "displayName": "job_attachments_test_no_settings_queue",
                        "createdAt": OLD,
                    },
                ]
            }
            mock_client.delete_queue.side_effect = [
                ClientError(
                    {"Error": {"Code": "ConflictException", "Message": "Queue in use"}},
                    "DeleteQueue",
                ),
                None,  # Second delete succeeds
            ]

            # WHEN / THEN - should not raise
            job_attachment_manager._cleanup_stale_queues()
            assert mock_client.delete_queue.call_count == 2

        def test_no_matching_queues_does_nothing(
            self,
            job_attachment_manager: JobAttachmentManager,
        ):
            """If no queues match our display names, nothing is deleted."""
            # GIVEN
            mock_client = job_attachment_manager.deadline_client._real_client
            mock_client.list_queues.return_value = {
                "queues": [
                    {
                        "queueId": "queue-other",
                        "displayName": "some_other_queue",
                        "createdAt": OLD,
                    },
                ]
            }

            # WHEN
            job_attachment_manager._cleanup_stale_queues()

            # THEN
            mock_client.delete_queue.assert_not_called()
