# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import botocore
import boto3
import os
import time
import pytest
import json
from typing import Any, Callable, Generator, Dict, Optional, Type
from types import TracebackType

from .deadline_manager import DeadlineManager
from .job_attachment_manager import JobAttachmentManager
from .utils import (
    generate_worker_role_cfn_template,
    generate_boostrap_worker_role_cfn_template,
    generate_boostrap_instance_profile_cfn_template,
    generate_queue_session_role,
    generate_job_attachments_bucket,
    generate_job_attachments_bucket_policy,
)

from .constants import (
    DEADLINE_WORKER_ROLE,
    DEADLINE_WORKER_BOOTSTRAP_ROLE,
    DEADLINE_WORKER_BOOSTRAP_INSTANCE_PROFILE_NAME,
    DEADLINE_QUEUE_SESSION_ROLE,
    DEADLINE_SERVICE_MODEL_BUCKET,
    CODEARTIFACT_DOMAIN,
    CODEARTIFACT_ACCOUNT_ID,
    CODEARTIFACT_REPOSITORY,
    JOB_ATTACHMENTS_BUCKET_NAME,
    JOB_ATTACHMENTS_BUCKET_RESOURCE,
    JOB_ATTACHMENTS_BUCKET_POLICY_RESOURCE,
    BOOTSTRAP_CLOUDFORMATION_STACK_NAME,
    STAGE,
)

AMI_ID = os.environ.get("AMI_ID", "")
SUBNET_ID = os.environ.get("SUBNET_ID", "")
SECURITY_GROUP_ID = os.environ.get("SECURITY_GROUP_ID", "")


@pytest.fixture(scope="session")
def stage() -> str:
    if os.getenv("LOCAL_DEVELOPMENT", "false").lower() == "true":
        return "dev"
    else:
        return os.environ["STAGE"]


@pytest.fixture(scope="session")
def account_id() -> str:
    return os.environ["SERVICE_ACCOUNT_ID"]


# Boto client fixtures
@pytest.fixture(scope="session")
def session() -> boto3.Session:
    return boto3.Session()


@pytest.fixture(scope="session")
def iam_client(session: boto3.Session) -> botocore.client.BaseClient:
    return session.client("iam")


@pytest.fixture(scope="session")
def ec2_client(session: boto3.Session) -> botocore.client.BaseClient:
    return session.client("ec2")


@pytest.fixture(scope="session")
def ssm_client(session: boto3.Session) -> botocore.client.BaseClient:
    return session.client("ssm")


@pytest.fixture(scope="session")
def cfn_client(session: boto3.Session) -> botocore.client.BaseClient:
    return session.client("cloudformation")


# Bootstrap persistent resources
@pytest.fixture(
    scope="session", autouse=os.environ.get("SKIP_BOOTSTRAP_TEST_RESOURCES", "False") != "True"
)
def bootstrap_test_resources(cfn_client: botocore.client.BaseClient) -> None:
    # All required resources are created using CloudFormation stack
    cfn_template: dict[str, Any] = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Stack created by deadline-cloud-test-fixtures",
        "Resources": {
            # A role for use by the Worker Agent after being bootstrapped
            DEADLINE_WORKER_ROLE: generate_worker_role_cfn_template(),
            DEADLINE_WORKER_BOOTSTRAP_ROLE: generate_boostrap_worker_role_cfn_template(),
            DEADLINE_QUEUE_SESSION_ROLE: generate_queue_session_role(),
            DEADLINE_WORKER_BOOSTRAP_INSTANCE_PROFILE_NAME: generate_boostrap_instance_profile_cfn_template(),
            JOB_ATTACHMENTS_BUCKET_RESOURCE: generate_job_attachments_bucket(),
            JOB_ATTACHMENTS_BUCKET_POLICY_RESOURCE: generate_job_attachments_bucket_policy(),
        },
    }
    stack_name = BOOTSTRAP_CLOUDFORMATION_STACK_NAME
    update_or_create_cfn_stack(cfn_client, stack_name, cfn_template)


# create or update bootstrap
def update_or_create_cfn_stack(
    cfn_client: botocore.client.BaseClient, stack_name: str, cfn_template: Dict[str, Any]
) -> None:
    try:
        cfn_client.update_stack(
            StackName=stack_name,
            TemplateBody=json.dumps(cfn_template),
            Capabilities=["CAPABILITY_NAMED_IAM"],
        )
        waiter = cfn_client.get_waiter("stack_update_complete")
        waiter.wait(StackName=stack_name)
    except cfn_client.exceptions.ClientError as e:
        if e.response["Error"]["Message"] != "No updates are to be performed.":
            cfn_client.create_stack(
                StackName=stack_name,
                TemplateBody=json.dumps(cfn_template),
                Capabilities=["CAPABILITY_NAMED_IAM"],
                OnFailure="DELETE",
                EnableTerminationProtection=False,
            )
            waiter = cfn_client.get_waiter("stack_create_complete")
            waiter.wait(StackName=stack_name)


@pytest.fixture(scope="session")
def deadline_manager_fixture():
    deadline_manager_fixture = DeadlineManager(should_add_deadline_models=True)
    yield deadline_manager_fixture


# get the worker role arn
@pytest.fixture(scope="session")
def worker_role_arn(iam_client: botocore.client.BaseClient) -> str:
    response = iam_client.get_role(RoleName=DEADLINE_WORKER_ROLE)
    return response["Role"]["Arn"]


@pytest.fixture(scope="session")
def deadline_scaffolding(
    deadline_manager_fixture: DeadlineManager, worker_role_arn: str
) -> Generator[Any, None, None]:
    deadline_manager_fixture.create_scaffolding(worker_role_arn, JOB_ATTACHMENTS_BUCKET_NAME)

    yield deadline_manager_fixture

    deadline_manager_fixture.cleanup_scaffolding()


@pytest.fixture(scope="session")
def launch_instance(ec2_client: botocore.client.BaseClient) -> Generator[Any, None, None]:
    with _InstanceLauncher(
        ec2_client,
        AMI_ID,
        SUBNET_ID,
        SECURITY_GROUP_ID,
        DEADLINE_WORKER_BOOSTRAP_INSTANCE_PROFILE_NAME,
    ) as instance_id:
        yield instance_id


@pytest.fixture(scope="session")
def create_worker_agent(
    deadline_scaffolding, launch_instance: str, send_ssm_command: Callable
) -> Generator[Any, None, None]:
    def configure_worker_agent_func() -> Dict:
        """Creates a Deadline Farm, starts an instance and configures and starts a Worker Agent."""
        assert deadline_scaffolding
        assert launch_instance

        configuration_command_response = send_ssm_command(
            launch_instance,
            (
                f"adduser -r -m agentuser && \n"
                f"adduser -r -m jobuser && \n"
                f"usermod -a -G jobuser agentuser && \n"
                f"chmod 770 /home/jobuser && \n"
                f"touch /etc/sudoers.d/deadline-worker-job-user && \n"
                f'echo "agentuser ALL=(jobuser) NOPASSWD:ALL" | sudo tee /etc/sudoers.d/deadline-worker-job-user && \n'
                f"python3.9 -m venv /opt/deadline/worker && \n"
                f"source /opt/deadline/worker/bin/activate && \n"
                f"pip install --upgrade pip && \n"
                f"touch /opt/deadline/worker/pip.conf && \n"
                # TODO: Remove when pypi is available
                f"aws codeartifact login --tool pip --domain {CODEARTIFACT_DOMAIN} --domain-owner {CODEARTIFACT_ACCOUNT_ID} --repository {CODEARTIFACT_REPOSITORY} && \n"
                f"aws s3 cp s3://{DEADLINE_SERVICE_MODEL_BUCKET}/service-2.json /tmp/deadline-beta-2020-08-21.json && \n"
                f"chmod +r /tmp/deadline-beta-2020-08-21.json && \n"
                f"sudo -u agentuser aws configure add-model  --service-model file:///tmp/deadline-beta-2020-08-21.json --service-name deadline && \n"
                f"mkdir /var/lib/deadline /var/log/amazon/deadline/ && \n"
                f"chown agentuser:agentuser /var/lib/deadline /var/log/amazon/deadline/ && \n"
                f"pip install deadline-worker-agent && \n"
                f"sudo -u agentuser /opt/deadline/worker/bin/deadline_worker_agent --help"
            ),
        )

        return configuration_command_response

    def start_worker_agent_func() -> Dict:
        start_command_response = send_ssm_command(
            launch_instance,
            (
                f"nohup sudo -E AWS_DEFAULT_REGION=us-west-2 -u agentuser /opt/deadline/worker/bin/deadline_worker_agent --farm-id {deadline_scaffolding.farm_id} --fleet-id {deadline_scaffolding.fleet_id} --allow-instance-profile >/dev/null 2>&1 &"
            ),
        )

        return start_command_response

    configuration_result = configure_worker_agent_func()

    assert configuration_result["ResponseCode"] == 0

    run_worker = start_worker_agent_func()

    assert run_worker["ResponseCode"] == 0

    yield run_worker


@pytest.fixture(scope="session")
def send_ssm_command(ssm_client: botocore.client.BaseClient) -> Callable:
    def send_ssm_command_func(instance_id: str, command: str) -> Dict:
        """Helper function to send single commands via SSM to a shell on a launched EC2 instance. Once the command has fully
        finished the result of the invocation is returned.
        """
        ssm_waiter = ssm_client.get_waiter("command_executed")

        # To successfully send an SSM Command to an instance the instance must:
        #  1) Be in RUNNING state;
        #  2) Have the AWS Systems Manager (SSM) Agent running; and
        #  3) Have had enough time for the SSM Agent to connect to System's Manager
        #
        # If we send an SSM command then we will get an InvalidInstanceId error
        # if the instance isn't in that state.
        NUM_RETRIES = 10
        SLEEP_INTERVAL_S = 5
        for i in range(0, NUM_RETRIES):
            try:
                send_command_response = ssm_client.send_command(
                    InstanceIds=[instance_id],
                    DocumentName="AWS-RunShellScript",
                    Parameters={"commands": [command]},
                )
                # Successfully sent. Bail out of the loop.
                break
            except botocore.exceptions.ClientError as error:
                error_code = error.response["Error"]["Code"]
                if error_code == "InvalidInstanceId" and i < NUM_RETRIES - 1:
                    time.sleep(SLEEP_INTERVAL_S)
                    continue
                raise

        command_id = send_command_response["Command"]["CommandId"]

        ssm_waiter.wait(InstanceId=instance_id, CommandId=command_id)
        ssm_command_result = ssm_client.get_command_invocation(
            InstanceId=instance_id, CommandId=command_id
        )

        return ssm_command_result

    return send_ssm_command_func


@pytest.fixture(scope="session")
def job_attachment_manager_fixture(stage: str, account_id: str):
    job_attachment_manager = JobAttachmentManager(stage, account_id)
    yield job_attachment_manager


@pytest.fixture(scope="session")
def deploy_job_attachment_resources(job_attachment_manager_fixture: JobAttachmentManager):
    job_attachment_manager_fixture.deploy_resources()
    yield job_attachment_manager_fixture
    job_attachment_manager_fixture.cleanup_resources()


class _InstanceLauncher:
    ami_id: str
    subnet_id: str
    security_group_id: str
    instance_profile_name: str
    instance_id: str
    ec2_client: botocore.client.BaseClient

    def __init__(
        self,
        ec2_client: botocore.client.BaseClient,
        ami_id: str,
        subnet_id: str,
        security_group_id: str,
        instance_profile_name: str,
    ) -> None:
        self.ec2_client = ec2_client
        self.ami_id = ami_id
        self.subnet_id = subnet_id
        self.security_group_id = security_group_id
        self.instance_profile_name = instance_profile_name

    def __enter__(self) -> str:
        instance_running_waiter = self.ec2_client.get_waiter("instance_status_ok")

        run_instance_response = self.ec2_client.run_instances(
            MinCount=1,
            MaxCount=1,
            ImageId=self.ami_id,
            InstanceType="t3.micro",
            IamInstanceProfile={"Name": self.instance_profile_name},
            SubnetId=self.subnet_id,
            SecurityGroupIds=[self.security_group_id],
            MetadataOptions={"HttpTokens": "required", "HttpEndpoint": "enabled"},
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": "InstanceIdentification", "Value": f"TestScaffolding{STAGE}"}],
                }
            ],
        )

        self.instance_id = run_instance_response["Instances"][0]["InstanceId"]

        instance_running_waiter.wait(InstanceIds=[self.instance_id])
        return self.instance_id

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.ec2_client.terminate_instances(InstanceIds=[self.instance_id])
