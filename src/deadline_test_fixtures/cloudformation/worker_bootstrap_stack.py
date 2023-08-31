# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

from .cfn import (
    Bucket,
    BucketPolicy,
    CfnResource,
    CfnStack,
    InstanceProfile,
    Role,
)
from ..models import CodeArtifactRepositoryInfo


class WorkerBootstrapStack(CfnStack):  # pragma: no cover
    worker_role: Role
    worker_bootstrap_role: Role
    worker_instance_profile: InstanceProfile
    session_role: Role
    job_attachments_bucket: Bucket
    job_attachments_bucket_policy: CfnResource
    bootstrap_bucket: Bucket
    bootstrap_bucket_policy: CfnResource

    def __init__(
        self,
        *,
        name: str,
        account: str,
        credential_vending_service_principal: str,
        codeartifact: CodeArtifactRepositoryInfo,
        description: str | None = None,
        service_model_s3_object_arn: str | None = None,
    ) -> None:
        super().__init__(
            name=name,
            description=description,
            capabilities=["CAPABILITY_NAMED_IAM"],
        )

        self.bootstrap_bucket = Bucket(
            self,
            "BootstrapBucket",
            bucket_name=f"deadline-scaffolding-worker-bootstrap-{account}",
            update_replace_policy="Delete",
            deletion_policy="Delete",
        )

        self.bootstrap_bucket_policy = BucketPolicy(
            self,
            "BootstrapBucketPolicy",
            bucket=self.bootstrap_bucket,
            policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "s3:*",
                        "Effect": "Deny",
                        "Principal": "*",
                        "Resource": [
                            self.bootstrap_bucket.arn,
                            self.bootstrap_bucket.arn_for_objects(),
                        ],
                        "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                    },
                ],
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
                                    "deadline:GetWorkerIamCredentials",
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
                                ],
                                "Resource": [self.bootstrap_bucket.arn_for_objects()],
                            },
                            # Allows the worker to download service model
                            *(
                                [
                                    {
                                        "Action": ["s3:GetObject", "s3:HeadObject"],
                                        "Resource": [service_model_s3_object_arn],
                                        "Effect": "Allow",
                                    }
                                ]
                                if service_model_s3_object_arn
                                else []
                            ),
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

        self.job_attachments_bucket = Bucket(
            self,
            "JobAttachmentsBucket",
            bucket_name=f"deadline-scaffolding-worker-job-attachments-{account}",
            update_replace_policy="Delete",
            deletion_policy="Delete",
        )

        self.job_attachments_bucket_policy = BucketPolicy(
            self,
            "JobAttachmentsBucketPolicy",
            bucket=self.job_attachments_bucket,
            policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "s3:*",
                        "Effect": "Deny",
                        "Principal": "*",
                        "Resource": [
                            self.job_attachments_bucket.arn,
                            self.job_attachments_bucket.arn_for_objects(),
                        ],
                        "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                    },
                ],
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
