# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

from .cfn import (
    Bucket,
    BucketPolicy,
    CfnStack,
)


class JobAttachmentsBootstrapStack(CfnStack):  # pragma: no cover
    bucket: Bucket
    bucket_policy: BucketPolicy

    def __init__(
        self,
        *,
        name: str,
        bucket_name: str,
        description: str | None = None,
    ) -> None:
        super().__init__(name=name, description=description)

        self.bucket = Bucket(
            stack=self,
            logical_name="JobAttachmentBucket",
            bucket_name=bucket_name,
            versioning=False,
            encryption={
                "ServerSideEncryptionConfiguration": [
                    {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}},
                ],
            },
            update_replace_policy="Delete",
            deletion_policy="Delete",
        )

        self.bucket_policy = BucketPolicy(
            stack=self,
            logical_name="JobAttachmentBucketPolicy",
            bucket=self.bucket,
            policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "s3:*",
                        "Effect": "Deny",
                        "Principal": "*",
                        "Resource": [
                            self.bucket.arn,
                            self.bucket.arn_for_objects(),
                        ],
                        "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                    },
                ],
            },
        )
