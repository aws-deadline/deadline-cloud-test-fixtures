# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

from .cfn import (
    Bucket,
    BucketLogging,
    BucketPolicy,
    CfnStack,
)

"""
Creates an S3 bucket configured in a secure way:
- Versioning is on
- Server-side encryption is set to AES256 by default
- Enforces SSL
- Blocks public access to the bucket
- Creates another secure bucket to store access logs for the main bucket
"""


def create_secure_bucket(
    stack: CfnStack,
    logical_name: str,
    *,
    bucket_kwargs: dict,
    log_bucket_kwargs: dict | None = None,
) -> tuple[Bucket, Bucket, BucketPolicy]:
    secure_bucket_kwargs = {
        "versioning": True,
        "block_public_access": "ALL",
        "encryption": {
            "ServerSideEncryptionConfiguration": [
                {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}},
            ],
        },
    }
    log_bucket = Bucket(
        stack,
        f"{logical_name}LogBucket",
        deletion_policy="Retain",
        update_replace_policy="Retain",
        **{
            **(log_bucket_kwargs if log_bucket_kwargs else {}),
            **secure_bucket_kwargs,
        },
    )
    bucket = Bucket(
        stack,
        logical_name,
        logging=BucketLogging(
            destination_bucket=log_bucket,
            log_file_prefix="logs",
        ),
        **{
            **bucket_kwargs,
            **secure_bucket_kwargs,
        },
    )
    bucket_policy = BucketPolicy(
        stack,
        f"{logical_name}Policy",
        bucket=bucket,
        policy_document={
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "s3:*",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Resource": [
                        bucket.arn,
                        bucket.arn_for_objects(),
                    ],
                    "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                },
            ],
        },
    )

    return bucket, log_bucket, bucket_policy
