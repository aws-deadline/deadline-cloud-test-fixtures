# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

from .cfn import (
    Bucket,
    BucketPolicy,
    CfnStack,
)
from .util import create_secure_bucket


class JobAttachmentsBootstrapStack(CfnStack):  # pragma: no cover
    bucket: Bucket
    log_bucket: Bucket
    bucket_policy: BucketPolicy

    def __init__(
        self,
        *,
        name: str,
        bucket_name: str,
        description: str | None = None,
    ) -> None:
        super().__init__(name=name, description=description)

        self.bucket, self.log_bucket, self.bucket_policy = create_secure_bucket(
            self,
            "JobAttachmentBucket",
            bucket_kwargs={
                "bucket_name": bucket_name,
            },
            log_bucket_kwargs={
                "bucket_name": f"{bucket_name}-logs",
            },
        )
