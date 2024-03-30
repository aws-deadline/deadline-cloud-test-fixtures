# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import botocore.client
import botocore.exceptions
import json
import logging
import re
from dataclasses import dataclass
from typing import Literal

from ..util import clean_kwargs

LOG = logging.getLogger(__name__)


class CfnStack:
    name: str
    description: str
    _resources: list[CfnResource]
    _capabilities: list[str] | None

    def __init__(
        self, *, name: str, description: str | None = None, capabilities: list[str] | None = None
    ) -> None:
        self.name = name
        self.description = description or "Stack created by deadline-cloud-test-fixtures"
        self._resources = []
        self._capabilities = capabilities

    def deploy(
        self,
        *,
        cfn_client: botocore.client.BaseClient,
    ) -> None:
        LOG.info(f"Bootstrapping test resources by deploying CloudFormation stack {self.name}")
        LOG.info(f"Attempting to update stack {self.name}")
        kwargs = clean_kwargs(
            {
                "StackName": self.name,
                "TemplateBody": json.dumps(self.template),
                "Capabilities": self._capabilities,
            }
        )
        try:
            cfn_client.update_stack(**kwargs)
            waiter = cfn_client.get_waiter("stack_update_complete")
            waiter.wait(StackName=self.name)
            LOG.info("Stack update complete")
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Message"] == "No updates are to be performed.":
                LOG.info("Stack is already up to date")
            elif re.match(r"Stack.+does not exist", e.response["Error"]["Message"]):
                LOG.info(f"Stack {self.name} does not exist yet. Creating new stack.")
                cfn_client.create_stack(
                    **kwargs,
                    OnFailure="DELETE",
                    EnableTerminationProtection=False,
                )
                waiter = cfn_client.get_waiter("stack_create_complete")
                waiter.wait(StackName=self.name)
                LOG.info("Stack create complete")
            else:
                LOG.exception(f"Unexpected error when attempting to update stack {self.name}: {e}")
                raise

    def destroy(self, *, cfn_client: botocore.client.BaseClient) -> None:
        cfn_client.delete_stack(StackName=self.name)

    def _add_resource(self, resource: CfnResource) -> None:  # pragma: no cover
        self._resources.append(resource)

    @property
    def template(self) -> dict:
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Description": self.description,
            "Resources": {r.logical_name: r.template for r in self._resources},
        }


class CfnResource:
    logical_name: str
    type: str
    properties: dict
    update_replace_policy: str | None
    deletion_policy: str | None

    def __init__(
        self,
        stack: CfnStack,
        type: str,
        logical_name: str,
        properties: dict,
        *,
        update_replace_policy: str | None = None,
        deletion_policy: str | None = None,
    ) -> None:
        self.logical_name = logical_name
        self.type = type
        self.properties = properties
        self.update_replace_policy = update_replace_policy
        self.deletion_policy = deletion_policy

        stack._add_resource(self)

    @property
    def template(self) -> dict:
        template = {
            "Type": self.type,
            "Properties": self.properties,
        }
        if self.update_replace_policy:
            template["UpdateReplacePolicy"] = self.update_replace_policy
        if self.deletion_policy:
            template["DeletionPolicy"] = self.deletion_policy
        return template

    @property
    def _physical_name_prop(self) -> str | None:
        return None

    @property
    def physical_name(self) -> str:
        if self._physical_name_prop is None:
            raise ValueError(f"Resource type {self.type} does not have a physical name")
        if self._physical_name_prop not in self.properties:
            raise ValueError(
                f"Physical name was not specified for this resource ({self.logical_name})"
            )
        return self.properties[self._physical_name_prop]

    @property
    def ref(self) -> dict:  # pragma: no cover
        return {"Ref": self.logical_name}

    def get_att(self, name: str) -> dict:  # pragma: no cover
        return {"Fn::GetAtt": [self.logical_name, name]}


@dataclass(frozen=True)
class BucketLogging:
    destination_bucket: Bucket | None = None
    log_file_prefix: str | None = None


class Bucket(CfnResource):  # pragma: no cover
    _physical_name_prop = "BucketName"

    def __init__(
        self,
        stack: CfnStack,
        logical_name: str,
        *,
        bucket_name: str | None = None,
        versioning: bool = False,
        encryption: dict | None = None,
        block_public_access: Literal["ALL"] | None = None,
        logging: BucketLogging | None = None,
        **kwargs,
    ) -> None:
        public_access_block_config = (
            {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            }
            if block_public_access == "ALL"
            else None
        )

        versioning_config = {"Status": "Enabled" if versioning else "Suspended"}

        logging_config = (
            clean_kwargs(
                {
                    "DestinationBucketName": (
                        logging.destination_bucket.ref if logging.destination_bucket else None
                    ),
                    "LogFilePrefix": logging.log_file_prefix,
                }
            )
            if logging
            else None
        )

        props = clean_kwargs(
            {
                "BucketName": bucket_name,
                "VersioningConfiguration": versioning_config,
                "PublicAccessBlockConfiguration": public_access_block_config,
                "BucketEncryption": encryption,
                "LoggingConfiguration": logging_config,
            }
        )
        super().__init__(stack, "AWS::S3::Bucket", logical_name, props, **kwargs)

    def arn_for_objects(self, *, pattern: str = "*") -> str:
        return f"{self.arn}/{pattern}"

    @property
    def arn(self) -> str:
        return f"arn:aws:s3:::{self.physical_name}"


class BucketPolicy(CfnResource):  # pragma: no cover
    def __init__(
        self,
        stack: CfnStack,
        logical_name: str,
        *,
        bucket: Bucket,
        policy_document: dict,
        **kwargs,
    ) -> None:
        props = clean_kwargs(
            {
                "Bucket": bucket.ref,
                "PolicyDocument": policy_document,
            }
        )
        super().__init__(stack, "AWS::S3::BucketPolicy", logical_name, props, **kwargs)


class Role(CfnResource):  # pragma: no cover
    _physical_name_prop = "RoleName"

    def __init__(
        self,
        stack: CfnStack,
        logical_name: str,
        *,
        assume_role_policy_document: dict,
        role_name: str | None = None,
        policies: list[dict] | None = None,
        managed_policy_arns: list[str] | None = None,
        **kwargs,
    ) -> None:
        props = clean_kwargs(
            {
                "AssumeRolePolicyDocument": assume_role_policy_document,
                "RoleName": role_name,
                "Policies": policies,
                "ManagedPolicyArns": managed_policy_arns,
            }
        )
        super().__init__(stack, "AWS::IAM::Role", logical_name, props, **kwargs)

    def format_arn(self, *, account: str) -> str:
        return f"arn:aws:iam::{account}:role/{self.physical_name}"


class InstanceProfile(CfnResource):  # pragma: no cover
    _physical_name_prop = "InstanceProfileName"

    def __init__(
        self,
        stack: CfnStack,
        logical_name: str,
        *,
        roles: list[Role],
        instance_profile_name: str | None = None,
        **kwargs,
    ) -> None:
        props = clean_kwargs(
            {
                "Roles": [role.ref for role in roles],
                "InstanceProfileName": instance_profile_name,
            }
        )
        super().__init__(stack, "AWS::IAM::InstanceProfile", logical_name, props, **kwargs)
