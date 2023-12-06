# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

from .cfn import (
    Bucket,
    CfnStack,
    InstanceProfile,
    Role,
)
from .util import create_secure_bucket
from ..models import CodeArtifactRepositoryInfo


class WorkerBootstrapStack(CfnStack):  # pragma: no cover
    worker_role: Role
    worker_bootstrap_role: Role
    worker_instance_profile: InstanceProfile
    session_role: Role
    job_attachments_bucket: Bucket
    bootstrap_bucket: Bucket

    def __init__(
        self,
        *,
        name: str,
        account: str,
        credential_vending_service_principal: str,
        codeartifact: CodeArtifactRepositoryInfo,
        description: str | None = None,
    ) -> None:
        super().__init__(
            name=name,
            description=description,
            capabilities=["CAPABILITY_NAMED_IAM"],
        )

        self.bootstrap_bucket, _, _ = create_secure_bucket(
            self,
            "BootstrapBucket",
            bucket_kwargs={
                "bucket_name": f"deadline-scaffolding-worker-bootstrap-{account}",
                "update_replace_policy": "Delete",
                "deletion_policy": "Delete",
            },
        )

        self.worker_role = Role(
            self,
            "WorkerRole",
            role_name="DeadlineScaffoldingWorkerRole",
            assume_role_policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": credential_vending_service_principal},
                        "Action": "sts:AssumeRole",
                    },
                ],
            },
            policies=[
                {
                    "PolicyName": "DeadlineScaffoldingWorkerRolePolicy",
                    "PolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "deadline:DeleteWorker",
                                    "deadline:BatchGetJobEntity",
                                    "deadline:AssumeQueueRoleForWorker",
                                    "deadline:AssumeFleetRoleForWorker",
                                    "deadline:UpdateWorkerSchedule",
                                    "deadline:UpdateWorker",
                                ],
                                "Resource": "*",
                            },
                            {
                                "Effect": "Allow",
                                "Action": [
                                    # Allows the read permissions via credentials
                                    "logs:GetLogEvents",
                                    # Allows the worker to push logs to CWL
                                    "logs:PutLogEvents",
                                    # Allows the worker to pass credentials to create log streams
                                    "logs:CreateLogStream",
                                ],
                                "Resource": "arn:aws:logs:*:*:*:/aws/deadline/*",
                            },
                            {
                                # For uploading logs and synchronizing job attachments
                                # Equivalent actions to CDK's Bucket.grantReadWrite for "deadline-" buckets
                                "Effect": "Allow",
                                "Action": [
                                    "s3:GetObject*",
                                    "s3:GetBucket*",
                                    "s3:List*",
                                    "s3:DeleteObject*",
                                    "s3:PutObject",
                                    "s3:PutObjectLegalHold",
                                    "s3:PutObjectRetention",
                                    "s3:PutObjectTagging",
                                    "s3:PutObjectVersionTagging",
                                    "s3:Abort*",
                                ],
                                "Resource": ["arn:aws:s3:::deadline-*"],
                            },
                        ],
                    },
                },
            ],
        )

        self.worker_bootstrap_role = Role(
            self,
            "WorkerBootstrapRole",
            role_name="DeadlineScaffoldingWorkerBootstrapRole",
            assume_role_policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "ec2.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    },
                ],
            },
            managed_policy_arns=[
                "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
            ],
            policies=[
                {
                    "PolicyName": "DeadlineScaffoldingWorkerBootstrapRolePolicy",
                    "PolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            # Allows the worker to bootstrap itself and grab the correct credentials
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "deadline:CreateWorker",
                                    "deadline:AssumeFleetRoleForWorker",
                                ],
                                "Resource": "*",
                            },
                            # Allow access to bootstrap bucket
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "s3:HeadObject",
                                    "s3:GetObject",
                                    "s3:ListBucket",
                                ],
                                "Resource": [
                                    self.bootstrap_bucket.arn,
                                    self.bootstrap_bucket.arn_for_objects(),
                                ],
                            },
                            # Allows access to code artifact
                            {
                                "Action": ["codeartifact:GetAuthorizationToken"],
                                "Resource": [codeartifact.domain_arn],
                                "Effect": "Allow",
                            },
                            {
                                "Action": ["sts:GetServiceBearerToken"],
                                "Resource": "*",
                                "Effect": "Allow",
                            },
                            {
                                "Action": [
                                    "codeartifact:ReadFromRepository",
                                    "codeartifact:GetRepositoryEndpoint",
                                ],
                                "Resource": [codeartifact.repository_arn],
                                "Effect": "Allow",
                            },
                        ],
                    },
                },
            ],
        )

        self.worker_instance_profile = InstanceProfile(
            self,
            "WorkerBootstrapInstanceProfile",
            instance_profile_name="DeadlineScaffoldingWorkerBootstrapInstanceProfile",
            roles=[self.worker_bootstrap_role],
        )

        self.job_attachments_bucket, _, _ = create_secure_bucket(
            self,
            "JobAttachmentsBucket",
            bucket_kwargs={
                "bucket_name": f"deadline-scaffolding-worker-job-attachments-{account}",
                "update_replace_policy": "Delete",
                "deletion_policy": "Delete",
            },
        )

        self.session_role = Role(
            self,
            "SessionRole",
            role_name="DeadlineScaffoldingWorkerSessionRole",
            assume_role_policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": credential_vending_service_principal},
                        "Action": "sts:AssumeRole",
                    },
                ],
            },
            policies=[
                {
                    "PolicyName": "DeadlineScaffoldingWorkerSessionRolePolicy",
                    "PolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "s3:GetObject",
                                    "s3:PutObject",
                                    "s3:ListBucket",
                                    "s3:GetBucketLocation",
                                ],
                                "Resource": [
                                    self.job_attachments_bucket.arn,
                                    self.job_attachments_bucket.arn_for_objects(),
                                ],
                            },
                        ],
                    },
                },
            ],
        )
