# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from .constants import (
    DEADLINE_WORKER_ROLE,
    DEADLINE_WORKER_BOOTSTRAP_ROLE,
    DEADLINE_WORKER_BOOSTRAP_INSTANCE_PROFILE_NAME,
    JOB_ATTACHMENTS_BUCKET_NAME,
    JOB_ATTACHMENTS_BUCKET_RESOURCE,
    DEADLINE_SERVICE_MODEL_BUCKET,
    CODEARTIFACT_DOMAIN,
    CODEARTIFACT_ACCOUNT_ID,
    CODEARTIFACT_REPOSITORY,
    DEADLINE_QUEUE_SESSION_ROLE,
    CREDENTIAL_VENDING_PRINCIPAL,
)

from typing import Any, Dict


# IAM Roles
def generate_boostrap_worker_role_cfn_template() -> Dict[str, Any]:
    cfn_template = {
        "Type": "AWS::IAM::Role",
        "Properties": {
            "RoleName": DEADLINE_WORKER_BOOTSTRAP_ROLE,
            "Description": DEADLINE_WORKER_BOOTSTRAP_ROLE,
            "AssumeRolePolicyDocument": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "ec2.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            },
            "Policies": [
                {
                    "PolicyName": f"{DEADLINE_WORKER_BOOTSTRAP_ROLE}Policy",
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
                            # Allows the worker to download service model
                            {
                                "Action": ["s3:GetObject", "s3:HeadObject"],
                                "Resource": [
                                    f"arn:aws:s3:::{DEADLINE_SERVICE_MODEL_BUCKET}/service-2.json"
                                ],
                                "Effect": "Allow",
                            },
                            # Allows access to code artifact
                            {
                                "Action": ["codeartifact:GetAuthorizationToken"],
                                "Resource": [
                                    f"arn:aws:codeartifact:us-west-2:{CODEARTIFACT_ACCOUNT_ID}:domain/{CODEARTIFACT_DOMAIN}"
                                ],
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
                                "Resource": [
                                    f"arn:aws:codeartifact:us-west-2:{CODEARTIFACT_ACCOUNT_ID}:repository/{CODEARTIFACT_DOMAIN}/{CODEARTIFACT_REPOSITORY}"
                                ],
                                "Effect": "Allow",
                            },
                        ],
                    },
                },
            ],
            "ManagedPolicyArns": ["arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"],
        },
    }
    return cfn_template


def generate_boostrap_instance_profile_cfn_template() -> Dict[str, Any]:
    cfn_template = {
        "Type": "AWS::IAM::InstanceProfile",
        "Properties": {
            "InstanceProfileName": DEADLINE_WORKER_BOOSTRAP_INSTANCE_PROFILE_NAME,
            "Roles": [
                {"Ref": DEADLINE_WORKER_BOOTSTRAP_ROLE},
            ],
        },
    }
    return cfn_template


def generate_worker_role_cfn_template() -> Dict[str, Any]:
    """This role matches the worker role of the closed-beta console"""
    cfn_template = {
        "Type": "AWS::IAM::Role",
        "Properties": {
            "RoleName": DEADLINE_WORKER_ROLE,
            "Description": DEADLINE_WORKER_ROLE,
            "AssumeRolePolicyDocument": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": CREDENTIAL_VENDING_PRINCIPAL},
                        "Action": "sts:AssumeRole",
                    }
                ],
            },
            "Policies": [
                {
                    "PolicyName": f"{DEADLINE_WORKER_ROLE}Policy",
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
        },
    }
    return cfn_template


def generate_queue_session_role() -> Dict[str, Any]:
    cfn_template = {
        "Type": "AWS::IAM::Role",
        "Properties": {
            "RoleName": DEADLINE_QUEUE_SESSION_ROLE,
            "Description": DEADLINE_QUEUE_SESSION_ROLE,
            "AssumeRolePolicyDocument": {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": CREDENTIAL_VENDING_PRINCIPAL},
                        "Action": "sts:AssumeRole",
                    }
                ],
            },
            "Policies": [
                {
                    "PolicyName": f"{DEADLINE_QUEUE_SESSION_ROLE}Policy",
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
                                    f"arn:aws:s3:::{JOB_ATTACHMENTS_BUCKET_NAME}"
                                    f"arn:aws:s3:::{JOB_ATTACHMENTS_BUCKET_NAME}/*"
                                ],
                            }
                        ],
                    },
                }
            ],
        },
    }

    return cfn_template


# Job Attachments Bucket
def generate_job_attachments_bucket() -> Dict[str, Any]:
    cfn_template = {
        "Type": "AWS::S3::Bucket",
        "Properties": {
            "BucketName": JOB_ATTACHMENTS_BUCKET_NAME,
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": [
                    {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                ]
            },
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
        },
        "UpdateReplacePolicy": "Delete",
        "DeletionPolicy": "Delete",
    }

    return cfn_template


def generate_job_attachments_bucket_policy() -> Dict[str, Any]:
    cfn_template = {
        "Type": "AWS::S3::BucketPolicy",
        "Properties": {
            "Bucket": {"Ref": JOB_ATTACHMENTS_BUCKET_RESOURCE},
            "PolicyDocument": {
                "Statement": [
                    {
                        "Action": "s3:*",
                        "Effect": "Deny",
                        "Principal": "*",
                        "Resource": f"arn:aws:s3:::{JOB_ATTACHMENTS_BUCKET_NAME}/*",
                        "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                    }
                ]
            },
        },
    }

    return cfn_template
