# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from typing import Generator
from unittest.mock import MagicMock

import boto3
import pytest
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from moto import mock_cloudformation

from deadline_test_fixtures.cloudformation.cfn import (
    CfnResource,
    CfnStack,
)


class TestCfnStack:
    @pytest.fixture(autouse=True)
    def config_caplog(self, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level("INFO")

    @pytest.fixture
    def stack(self) -> CfnStack:
        return CfnStack(
            name="TestStack",
            description="TestStack",
        )

    @pytest.fixture(autouse=True)
    def resource(self, stack: CfnStack) -> CfnResource:
        # Just use a bucket since moto actually "deploys" the stack, and:
        # - Custom resources actually need a backing lambda
        # - Unknown resource types are not supported (i.e. anything not Custom:: or a supported AWS:: type)
        return CfnResource(stack, "AWS::S3::Bucket", "Bucket", {})

    @pytest.fixture
    def cfn_client(self) -> Generator[BaseClient, None, None]:
        with mock_cloudformation():
            yield boto3.client("cloudformation")

    class TestDeploy:
        def test_deploys_stack(
            self,
            stack: CfnStack,
            cfn_client: BaseClient,
            resource: CfnResource,
            caplog: pytest.LogCaptureFixture,
        ) -> None:
            # WHEN
            stack.deploy(cfn_client=cfn_client)

            # THEN
            assert f"Stack {stack.name} does not exist yet. Creating new stack." in caplog.text
            stacks = cfn_client.describe_stacks(StackName=stack.name)["Stacks"]
            assert len(stacks) == 1
            assert stacks[0]["StackName"] == stack.name
            resources = cfn_client.describe_stack_resources(StackName=stack.name)["StackResources"]
            assert len(resources) == 1
            assert resources[0]["LogicalResourceId"] == resource.logical_name

        def test_updates_stack(
            self,
            stack: CfnStack,
            resource: CfnResource,
            cfn_client: BaseClient,
            caplog: pytest.LogCaptureFixture,
        ) -> None:
            # GIVEN
            stack.deploy(cfn_client=cfn_client)
            resource2 = CfnResource(stack, "AWS::S3::Bucket", "Bucket2", {})

            # WHEN
            stack.deploy(cfn_client=cfn_client)

            # THEN
            assert "Stack update complete" in caplog.text
            resources = cfn_client.describe_stack_resources(StackName=stack.name)["StackResources"]
            assert len(resources) == 2
            resources_logical_ids = [r["LogicalResourceId"] for r in resources]
            assert resource.logical_name in resources_logical_ids
            assert resource2.logical_name in resources_logical_ids

        def test_does_not_raise_when_stack_is_up_to_date(
            self,
            stack: CfnStack,
            caplog: pytest.LogCaptureFixture,
        ) -> None:
            # GIVEN
            # moto doesn't raise a ValidationError like CloudFormation does when calling UpdateStack
            # when there are no updates to be performed, so setup a mock instead
            mock_client = MagicMock()
            mock_client.update_stack.side_effect = ClientError(
                {"Error": {"Message": "No updates are to be performed."}},
                "UpdateStack",
            )

            try:
                # WHEN
                stack.deploy(cfn_client=mock_client)
            except Exception as e:
                pytest.fail(f"Stack.deploy() raised an error when it shouldn't have: {e}")
            else:
                # THEN
                assert "Stack is already up to date" in caplog.text

        @pytest.mark.parametrize(
            "error",
            [
                ClientError({"Error": {"Message": "test"}}, None),
                Exception(),
            ],
        )
        def test_raises_other_errors(
            self,
            error: Exception,
            stack: CfnStack,
            caplog: pytest.LogCaptureFixture,
        ) -> None:
            # GIVEN
            mock_client = MagicMock()
            mock_client.update_stack.side_effect = error

            with pytest.raises(type(error)) as raised_err:
                # WHEN
                stack.deploy(cfn_client=mock_client)

            # THEN
            assert raised_err.value is error
            if isinstance(error, ClientError):
                assert (
                    f"Unexpected error when attempting to update stack {stack.name}: "
                    in caplog.text
                )

    def test_destroy(
        self,
        stack: CfnStack,
        cfn_client: BaseClient,
    ) -> None:
        # GIVEN
        stack.deploy(cfn_client=cfn_client)
        stacks = cfn_client.describe_stacks(StackName=stack.name)["Stacks"]
        assert len(stacks) == 1
        assert stacks[0]["StackStatus"] == "CREATE_COMPLETE"

        # WHEN
        stack.destroy(cfn_client=cfn_client)

        # THEN
        # DescribeStacks API required Stack ID for deleted stacks, so just use ListStacks
        stacks = cfn_client.list_stacks()["StackSummaries"]
        assert len(stacks) == 1
        assert stacks[0]["StackStatus"] == "DELETE_COMPLETE"

    def test_template(self) -> None:
        # GIVEN
        stack = CfnStack(name="TheStack", description="TheDescription")
        resources = [
            CfnResource(stack, "AWS::S3::Bucket", "BucketA", {"A": "a"}),
            CfnResource(stack, "AWS::S3::Bucket", "BucketB", {"B": "b"}),
            CfnResource(stack, "AWS::S3::Bucket", "BucketC", {"C": "c"}),
        ]

        # WHEN
        template = stack.template

        # THEN
        assert template["Description"] == stack.description
        for resource in resources:
            assert resource.logical_name in template["Resources"]
            assert resource.template == template["Resources"][resource.logical_name]


class TestCfnResource:
    @pytest.fixture
    def stack(self) -> CfnStack:
        return CfnStack(name="TestStack")

    class TestTemplate:
        @pytest.fixture
        def resource_type(self) -> str:
            return "Test::Resource::Type"

        @pytest.fixture
        def resource_props(self) -> dict:
            return {
                "PropertyA": "ValueA",
                "PropertyB": {
                    "A": "a",
                    "B": "b",
                },
            }

        @pytest.fixture
        def resource(
            self,
            stack: CfnStack,
            resource_type: str,
            resource_props: dict,
        ) -> CfnResource:
            # GIVEN
            return CfnResource(stack, resource_type, "TestResource", resource_props)

        def test_defaults(
            self, resource: CfnResource, resource_type: str, resource_props: dict
        ) -> None:
            # THEN
            assert resource.template == {
                "Type": resource_type,
                "Properties": resource_props,
            }

        def test_applies_update_replace_policy(self, resource: CfnResource) -> None:
            # GIVEN
            resource.update_replace_policy = "Retain"

            # THEN
            assert resource.template["UpdateReplacePolicy"] == "Retain"

        def test_applies_deletion_policy(self, resource: CfnResource) -> None:
            # GIVEN
            resource.deletion_policy = "Retain"

            # THEN
            assert resource.template["DeletionPolicy"] == "Retain"

    class TestPhysicalName:
        def test_gets_physical_name(self, stack: CfnStack) -> None:
            # GIVEN
            class TestResource(CfnResource):
                _physical_name_prop = "TestName"

            resource = TestResource(
                stack,
                "Test::Resource::Type",
                "TestResource",
                {
                    "TestName": "PhysicalName",
                },
            )

            # THEN
            assert resource.physical_name == "PhysicalName"

        def test_raises_when_no_physical_name(self, stack: CfnStack) -> None:
            # GIVEN
            resource = CfnResource(
                stack,
                "Test::Resource::Type",
                "TestResource",
                {
                    "TestName": "PhysicalName",
                },
            )

            with pytest.raises(ValueError) as raised_err:
                # WHEN
                resource.physical_name

            # THEN
            assert (
                str(raised_err.value)
                == "Resource type Test::Resource::Type does not have a physical name"
            )

        def test_raises_when_physical_name_not_in_properties(self, stack: CfnStack) -> None:
            # GIVEN
            class TestResource(CfnResource):
                _physical_name_prop = "TestName"

            resource = TestResource(stack, "Test::Resource::Type", "TestResource", {})

            with pytest.raises(ValueError) as raised_err:
                # WHEN
                resource.physical_name

            # THEN
            assert (
                str(raised_err.value)
                == "Physical name was not specified for this resource (TestResource)"
            )

    def test_init_adds_to_stack(self, stack: CfnStack) -> None:
        # WHEN
        resource = CfnResource(stack, "Test::Resource::Type", "TestResource", {})

        # THEN
        assert resource in stack._resources
