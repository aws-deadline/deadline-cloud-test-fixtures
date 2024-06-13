# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import abc
import botocore.client
import botocore.exceptions
import glob
import json
import logging
import os
import pathlib
import posixpath
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field, InitVar, replace
from typing import Any, ClassVar, Optional, cast

from ..models import (
    PipInstall,
    PosixSessionUser,
    OperatingSystem,
)
from ..util import call_api, wait_for

LOG = logging.getLogger(__name__)

# Hardcoded to default posix path for worker.json file which has the worker ID in it
WORKER_JSON_PATH = "/var/lib/deadline/worker.json"
DOCKER_CONTEXT_DIR = os.path.join(os.path.dirname(__file__), "..", "containers", "worker")


def linux_worker_command(config: DeadlineWorkerConfiguration) -> str:  # pragma: no cover
    """Get the command to configure the Worker. This must be run as root."""

    cmds = [
        config.worker_agent_install.install_command_for_linux,
        *(config.pre_install_commands or []),
        # fmt: off
        (
            "install-deadline-worker "
            + "-y "
            + f"--farm-id {config.farm_id} "
            + f"--fleet-id {config.fleet_id} "
            + f"--region {config.region} "
            + f"--user {config.user} "
            + f"--group {config.group} "
            + f"{'--allow-shutdown ' if config.allow_shutdown else ''}"
            + f"{'--no-install-service ' if config.no_install_service else ''}"
            + f"{'--start ' if config.start_service else ''}"
        ),
        # fmt: on
    ]

    if config.service_model_path:
        cmds.append(
            f"runuser -l {config.user} -s /bin/bash -c 'aws configure add-model --service-model file://{config.service_model_path}'"
        )

    if os.environ.get("AWS_ENDPOINT_URL_DEADLINE"):
        LOG.info(f"Using AWS_ENDPOINT_URL_DEADLINE: {os.environ.get('AWS_ENDPOINT_URL_DEADLINE')}")
        cmds.insert(
            0,
            f"runuser -l {config.user} -s /bin/bash -c 'echo export AWS_ENDPOINT_URL_DEADLINE={os.environ.get('AWS_ENDPOINT_URL_DEADLINE')} >> ~/.bashrc'",
        )

    return " && ".join(cmds)


def windows_worker_command(config: DeadlineWorkerConfiguration) -> str:  # pragma: no cover
    """Get the command to configure the Worker. This must be run as root."""

    cmds = [
        config.worker_agent_install.install_command_for_windows,
        *(config.pre_install_commands or []),
        # fmt: off
        (
            "install-deadline-worker "
            + "-y "
            + f"--farm-id {config.farm_id} "
            + f"--fleet-id {config.fleet_id} "
            + f"--region {config.region} "
            + f"--user {config.user} "
            + f"{'--allow-shutdown ' if config.allow_shutdown else ''}"
            + "--start"
        ),
        # fmt: on
    ]

    if config.service_model_path:
        cmds.append(
            f"aws configure add-model --service-model file://{config.service_model_path} --service-name deadline; "
            f"Copy-Item -Path ~\\.aws\\* -Destination C:\\Users\\Administrator\\.aws\\models -Recurse; "
            f"Copy-Item -Path ~\\.aws\\* -Destination C:\\Users\\{config.user}\\.aws\\models -Recurse; "
            f"Copy-Item -Path ~\\.aws\\* -Destination C:\\Users\\jobuser\\.aws\\models -Recurse"
        )

    if os.environ.get("AWS_ENDPOINT_URL_DEADLINE"):
        LOG.info(f"Using AWS_ENDPOINT_URL_DEADLINE: {os.environ.get('AWS_ENDPOINT_URL_DEADLINE')}")
        cmds.insert(
            0,
            f"[System.Environment]::SetEnvironmentVariable('AWS_ENDPOINT_URL_DEADLINE', '{os.environ.get('AWS_ENDPOINT_URL_DEADLINE')}', [System.EnvironmentVariableTarget]::Machine); "
            "$env:AWS_ENDPOINT_URL_DEADLINE = [System.Environment]::GetEnvironmentVariable('AWS_ENDPOINT_URL_DEADLINE','Machine')",
        )

    return "; ".join(cmds)


def configure_worker_command(*, config: DeadlineWorkerConfiguration) -> str:  # pragma: no cover
    """Get the command to configure the Worker. This must be run as root."""

    if config.operating_system.name == "AL2023":
        return linux_worker_command(config)
    else:
        return windows_worker_command(config)


class DeadlineWorker(abc.ABC):
    @abc.abstractmethod
    def start(self) -> None:
        pass

    @abc.abstractmethod
    def stop(self) -> None:
        pass

    @abc.abstractmethod
    def send_command(self, command: str) -> CommandResult:
        pass

    @abc.abstractproperty
    def worker_id(self) -> str:
        pass


@dataclass(frozen=True)
class CommandResult:  # pragma: no cover
    exit_code: int
    stdout: str
    stderr: Optional[str] = None

    def __str__(self) -> str:
        return "\n".join(
            [
                f"exit_code: {self.exit_code}",
                "",
                "================================",
                "========= BEGIN stdout =========",
                "================================",
                "",
                self.stdout,
                "",
                "==============================",
                "========= END stdout =========",
                "==============================",
                "",
                "================================",
                "========= BEGIN stderr =========",
                "================================",
                "",
                str(self.stderr),
                "",
                "==============================",
                "========= END stderr =========",
                "==============================",
            ]
        )


@dataclass(frozen=True)
class DeadlineWorkerConfiguration:
    operating_system: OperatingSystem
    farm_id: str
    fleet_id: str
    region: str
    user: str
    group: str
    allow_shutdown: bool
    worker_agent_install: PipInstall
    job_users: list[PosixSessionUser] = field(
        default_factory=lambda: [PosixSessionUser("jobuser", "jobuser")]
    )
    start_service: bool = False
    no_install_service: bool = False
    service_model_path: str | None = None
    file_mappings: list[tuple[str, str]] | None = None
    """Mapping of files to copy from host environment to worker environment"""
    pre_install_commands: list[str] | None = None
    """Commands to run before installing the Worker agent"""


@dataclass
class EC2InstanceWorker(DeadlineWorker):
    AL2023_AMI_NAME: ClassVar[str] = "al2023-ami-kernel-6.1-x86_64"
    WIN2022_AMI_NAME: ClassVar[str] = "Windows_Server-2022-English-Full-Base"

    subnet_id: str
    security_group_id: str
    instance_profile_name: str
    bootstrap_bucket_name: str
    s3_client: botocore.client.BaseClient
    ec2_client: botocore.client.BaseClient
    ssm_client: botocore.client.BaseClient
    configuration: DeadlineWorkerConfiguration

    instance_id: Optional[str] = field(init=False, default=None)

    override_ami_id: InitVar[Optional[str]] = None
    """
    Option to override the AMI ID for the EC2 instance. The latest AL2023 is used by default.
    Note that the scripting to configure the EC2 instance is only verified to work on AL2023.
    """

    def __post_init__(self, override_ami_id: Optional[str] = None):
        if override_ami_id:
            self._ami_id = override_ami_id

    def start(self) -> None:
        s3_files = self._stage_s3_bucket()
        self._launch_instance(s3_files=s3_files)
        self._start_worker_agent()

    def stop(self) -> None:
        LOG.info(f"Terminating EC2 instance {self.instance_id}")
        self.ec2_client.terminate_instances(InstanceIds=[self.instance_id])
        self.instance_id = None

    def send_command(self, command: str) -> CommandResult:
        """Send a command via SSM to a shell on a launched EC2 instance. Once the command has fully
        finished the result of the invocation is returned.
        """
        ssm_waiter = self.ssm_client.get_waiter("command_executed")

        # To successfully send an SSM Command to an instance the instance must:
        #  1) Be in RUNNING state;
        #  2) Have the AWS Systems Manager (SSM) Agent running; and
        #  3) Have had enough time for the SSM Agent to connect to System's Manager
        #
        # If we send an SSM command then we will get an InvalidInstanceId error
        # if the instance isn't in that state.
        NUM_RETRIES = 20
        SLEEP_INTERVAL_S = 10
        for i in range(0, NUM_RETRIES):
            LOG.info(f"Sending SSM command to instance {self.instance_id}")
            try:
                if self.configuration.operating_system.name == "AL2023":
                    send_command_response = self.ssm_client.send_command(
                        InstanceIds=[self.instance_id],
                        DocumentName="AWS-RunShellScript",
                        Parameters={"commands": [command]},
                    )
                else:
                    send_command_response = self.ssm_client.send_command(
                        InstanceIds=[self.instance_id],
                        DocumentName="AWS-RunPowerShellScript",
                        Parameters={"commands": [command]},
                    )
                break
            except botocore.exceptions.ClientError as error:
                error_code = error.response["Error"]["Code"]
                if error_code == "InvalidInstanceId" and i < NUM_RETRIES - 1:
                    LOG.warning(
                        f"Instance {self.instance_id} is not ready for SSM command (received InvalidInstanceId error). Retrying in {SLEEP_INTERVAL_S}s."
                    )
                    time.sleep(SLEEP_INTERVAL_S)
                    continue
                raise

        command_id = send_command_response["Command"]["CommandId"]

        LOG.info(f"Waiting for SSM command {command_id} to reach a terminal state")
        try:
            ssm_waiter.wait(
                InstanceId=self.instance_id,
                CommandId=command_id,
            )
        except botocore.exceptions.WaiterError:  # pragma: no cover
            # Swallow exception, we're going to check the result anyway
            pass

        ssm_command_result = self.ssm_client.get_command_invocation(
            InstanceId=self.instance_id,
            CommandId=command_id,
        )
        result = CommandResult(
            exit_code=ssm_command_result["ResponseCode"],
            stdout=ssm_command_result["StandardOutputContent"],
            stderr=ssm_command_result["StandardErrorContent"],
        )
        if result.exit_code == -1:  # pragma: no cover
            # Response code of -1 in a terminal state means the command was not received by the node
            LOG.error(f"Failed to send SSM command {command_id} to {self.instance_id}: {result}")

        LOG.info(f"SSM command {command_id} completed with exit code: {result.exit_code}")
        return result

    def _stage_s3_bucket(self) -> list[tuple[str, str]] | None:
        """Stages file_mappings to an S3 bucket and returns the mapping of S3 URI to dest path"""
        if not self.configuration.file_mappings:
            LOG.info("No file mappings to stage to S3")
            return None

        s3_to_src_mapping: dict[str, str] = {}
        s3_to_dst_mapping: dict[str, str] = {}
        for src_glob, dst in self.configuration.file_mappings:
            for src_file in glob.glob(src_glob):
                s3_key = f"worker/{os.path.basename(src_file)}"
                assert s3_key not in s3_to_src_mapping, (
                    "Duplicate S3 keys generated for file mappings. All source files must have unique "
                    + f"filenames. Mapping: {self.configuration.file_mappings}"
                )
                s3_to_src_mapping[s3_key] = src_file
                s3_to_dst_mapping[f"s3://{self.bootstrap_bucket_name}/{s3_key}"] = dst

        for key, local_path in s3_to_src_mapping.items():
            LOG.info(f"Uploading file {local_path} to s3://{self.bootstrap_bucket_name}/{key}")
            try:
                # self.s3_client.upload_file(local_path, self.bootstrap_bucket_name, key)
                with open(local_path, mode="rb") as f:
                    self.s3_client.put_object(
                        Bucket=self.bootstrap_bucket_name,
                        Key=key,
                        Body=f,
                    )
            except botocore.exceptions.ClientError as e:
                LOG.exception(
                    f"Failed to upload file {local_path} to s3://{self.bootstrap_bucket_name}/{key}: {e}"
                )
                raise

        return list(s3_to_dst_mapping.items())

    def linux_userdata(self, s3_files) -> str:
        copy_s3_command = ""
        job_users_cmds = []

        if s3_files:
            copy_s3_command = " && ".join(
                [
                    f"aws s3 cp {s3_uri} {dst} && chown {self.configuration.user} {dst}"
                    for s3_uri, dst in s3_files
                ]
            )
        for job_user in self.configuration.job_users:
            job_users_cmds.append(f"groupadd {job_user.group}")
            job_users_cmds.append(
                f"useradd --create-home --system --shell=/bin/bash --groups={self.configuration.group} -g {job_user.group} {job_user.user}"
            )
            job_users_cmds.append(f"usermod -a -G {job_user.group} {self.configuration.user}")

        sudoer_rule_users = ",".join(
            [
                self.configuration.user,
                *[job_user.user for job_user in self.configuration.job_users],
            ]
        )
        job_users_cmds.append(
            f'echo "{self.configuration.user} ALL=({sudoer_rule_users}) NOPASSWD: ALL" > /etc/sudoers.d/{self.configuration.user}'
        )

        configure_job_users = "\n".join(job_users_cmds)

        userdata = f"""#!/bin/bash
        # Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
        set -x
        groupadd --system {self.configuration.group}
        useradd --create-home --system --shell=/bin/bash --groups={self.configuration.group} {self.configuration.user}
        {configure_job_users}
        {copy_s3_command}

        runuser --login {self.configuration.user} --command 'python3 -m venv $HOME/.venv && echo ". $HOME/.venv/bin/activate" >> $HOME/.bashrc'
        """

        return userdata

    def windows_userdata(self, s3_files) -> str:
        copy_s3_command = ""
        if s3_files:
            copy_s3_command = " ; ".join([f"aws s3 cp {s3_uri} {dst}" for s3_uri, dst in s3_files])

        userdata = f"""<powershell>
            Invoke-WebRequest -Uri "https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe" -OutFile "C:\python-3.11.9-amd64.exe"
            Start-Process -FilePath "C:\python-3.11.9-amd64.exe" -ArgumentList "/quiet InstallAllUsers=1 PrependPath=1 AppendPath=1" -Wait
            Invoke-WebRequest -Uri "https://awscli.amazonaws.com/AWSCLIV2.msi" -Outfile "C:\AWSCLIV2.msi"
            Start-Process msiexec.exe -ArgumentList "/i C:\AWSCLIV2.msi /quiet" -Wait
            $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine")
            $secret = aws secretsmanager get-secret-value --secret-id WindowsPasswordSecret --query SecretString --output text | ConvertFrom-Json
            $password = ConvertTo-SecureString -String $($secret.password) -AsPlainText -Force
            New-LocalUser -Name "jobuser" -Password $password -FullName "jobuser" -Description "job user"
            $Cred = New-Object System.Management.Automation.PSCredential "jobuser", $password
            Start-Process cmd.exe -Credential $Cred -ArgumentList "/C" -LoadUserProfile -NoNewWindow
            {copy_s3_command}
            </powershell>"""

        return userdata

    def _launch_instance(self, *, s3_files: list[tuple[str, str]] | None = None) -> None:
        assert (
            not self.instance_id
        ), "Attempted to launch EC2 instance when one was already launched"

        if self.configuration.operating_system.name == "AL2023":
            userdata = self.linux_userdata(s3_files)
        else:
            userdata = self.windows_userdata(s3_files)

        LOG.info("Launching EC2 instance")
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
                    "Tags": [
                        {
                            "Key": "InstanceIdentification",
                            "Value": "DeadlineScaffoldingWorker",
                        }
                    ],
                }
            ],
            UserData=userdata,
        )

        self.instance_id = run_instance_response["Instances"][0]["InstanceId"]
        LOG.info(f"Launched EC2 instance {self.instance_id}")

        LOG.info(f"Waiting for EC2 instance {self.instance_id} status to be OK")
        instance_running_waiter = self.ec2_client.get_waiter("instance_status_ok")
        instance_running_waiter.wait(
            InstanceIds=[self.instance_id],
            WaiterConfig={"Delay": 15, "MaxAttempts": 60},
        )
        LOG.info(f"EC2 instance {self.instance_id} status is OK")

    def start_linux_worker(self) -> None:
        cmd_result = self.send_command(
            f"cd /home/{self.configuration.user}; . .venv/bin/activate; AWS_DEFAULT_REGION={self.configuration.region} {configure_worker_command(config=self.configuration)}"
        )
        assert cmd_result.exit_code == 0, f"Failed to configure Worker agent: {cmd_result}"
        LOG.info("Successfully configured Worker agent")

        LOG.info(f"Sending SSM command to start Worker agent on instance {self.instance_id}")
        cmd_result = self.send_command(
            " && ".join(
                [
                    f"nohup runuser --login {self.configuration.user} -c 'AWS_DEFAULT_REGION={self.configuration.region} deadline-worker-agent > /tmp/worker-agent-stdout.txt 2>&1 &'",
                    # Verify Worker is still running
                    "echo Waiting 5s for agent to get started",
                    "sleep 5",
                    "echo 'Running pgrep to see if deadline-worker-agent is running'",
                    f"pgrep --count --full -u {self.configuration.user} deadline-worker-agent",
                ]
            ),
        )
        assert cmd_result.exit_code == 0, f"Failed to start Worker agent: {cmd_result}"
        LOG.info("Successfully started Worker agent")

    def start_windows_worker(self) -> None:
        cmd_result = self.send_command(f"{configure_worker_command(config=self.configuration)}")
        LOG.info("Successfully configured Worker agent")
        LOG.info("Sending SSM Command to check if Worker Agent is running")
        cmd_result = self.send_command(
            " ; ".join(
                [
                    "echo Waiting 20s for the agent service to get started",
                    "sleep 20",
                    "echo 'Running running Get-Process to check if the agent is running'",
                    "IF(Get-Process pythonservice){echo 'service is running'}ELSE{exit 1}",
                ]
            ),
        )
        assert cmd_result.exit_code == 0, f"Failed to start Worker agent: {cmd_result}"
        LOG.info("Successfully started Worker agent")

    def _start_worker_agent(self) -> None:  # pragma: no cover
        assert self.instance_id

        LOG.info(f"Sending SSM command to configure Worker agent on instance {self.instance_id}")

        if self.configuration.operating_system.name == "AL2023":
            self.start_linux_worker()
        else:
            self.start_windows_worker()

    @property
    def worker_id(self) -> str:
        cmd_result = self.send_command("cat /var/lib/deadline/worker.json  | jq -r '.worker_id'")
        assert cmd_result.exit_code == 0, f"Failed to get Worker ID: {cmd_result}"

        worker_id = cmd_result.stdout.rstrip("\n\r")
        assert re.match(
            r"^worker-[0-9a-f]{32}$", worker_id
        ), f"Got nonvalid Worker ID from command stdout: {cmd_result}"
        return worker_id

    @property
    def ami_id(self) -> str:
        if not hasattr(self, "_ami_id"):
            if self.configuration.operating_system.name == "AL2023":
                # Grab the latest AL2023 AMI
                # https://aws.amazon.com/blogs/compute/query-for-the-latest-amazon-linux-ami-ids-using-aws-systems-manager-parameter-store/
                ssm_param_name = (
                    f"/aws/service/ami-amazon-linux-latest/{EC2InstanceWorker.AL2023_AMI_NAME}"
                )
            else:
                # Grab the latest Windows Server 2022 AMI
                # https://aws.amazon.com/blogs/mt/query-for-the-latest-windows-ami-using-systems-manager-parameter-store/
                ssm_param_name = (
                    f"/aws/service/ami-windows-latest/{EC2InstanceWorker.WIN2022_AMI_NAME}"
                )

            response = call_api(
                description=f"Getting latest {self.configuration.operating_system.name} AMI ID from SSM parameter {ssm_param_name}",
                fn=lambda: self.ssm_client.get_parameters(Names=[ssm_param_name]),
            )

            parameters = response.get("Parameters", [])
            assert (
                len(parameters) == 1
            ), f"Received incorrect number of SSM parameters. Expected 1, got response: {response}"
            self._ami_id = parameters[0]["Value"]
            LOG.info(f"Using latest {self.configuration.operating_system.name} AMI {self._ami_id}")

        return self._ami_id


@dataclass
class DockerContainerWorker(DeadlineWorker):
    configuration: DeadlineWorkerConfiguration

    _container_id: Optional[str] = field(init=False, default=None)

    def __post_init__(self) -> None:
        # Do not install Worker agent service since it's recommended to avoid systemd usage on Docker containers
        self.configuration = replace(self.configuration, no_install_service=True)

    def start(self) -> None:
        self._tmpdir = pathlib.Path(tempfile.mkdtemp())

        assert (
            len(self.configuration.job_users) == 1
        ), f"Multiple job users not supported on Docker worker: {self.configuration.job_users}"
        # Environment variables for "run_container.sh"
        run_container_env = {
            **os.environ,
            "FARM_ID": self.configuration.farm_id,
            "FLEET_ID": self.configuration.fleet_id,
            "AGENT_USER": self.configuration.user,
            "SHARED_GROUP": self.configuration.group,
            "JOB_USER": self.configuration.job_users[0].user,
            "CONFIGURE_WORKER_AGENT_CMD": configure_worker_command(
                config=self.configuration,
            ),
        }

        LOG.info(f"Staging Docker build context directory {str(self._tmpdir)}")
        shutil.copytree(DOCKER_CONTEXT_DIR, str(self._tmpdir), dirs_exist_ok=True)

        if self.configuration.file_mappings:
            # Stage a special dir with files to copy over to a temp folder in the Docker container
            # The container is responsible for copying files from that temp folder into the final destinations
            file_mappings_dir = self._tmpdir / "file_mappings"
            os.makedirs(str(file_mappings_dir))

            # Mapping of files in temp Docker container folder to their final destination
            docker_file_mappings: dict[str, str] = {}
            for src, dst in self.configuration.file_mappings:
                src_file_name = os.path.basename(src)

                # The Dockerfile copies the file_mappings dir in the build context to "/file_mappings" in the container
                # Build up an array of mappings from "/file_mappings" to their final destination
                src_docker_path = posixpath.join("/file_mappings", src_file_name)
                assert src_docker_path not in docker_file_mappings, (
                    "Duplicate paths generated for file mappings. All source files must have unique "
                    + f"filenames. Mapping: {self.configuration.file_mappings}"
                )
                docker_file_mappings[src_docker_path] = dst

                # Copy the file over to the stage directory
                staged_dst = str(file_mappings_dir / src_file_name)
                LOG.info(f"Copying file {src} to {staged_dst}")
                shutil.copyfile(src, staged_dst)

            run_container_env["FILE_MAPPINGS"] = json.dumps(docker_file_mappings)

        # Build and start the container
        LOG.info("Starting Docker container")
        try:
            proc = subprocess.Popen(
                args="./run_container.sh",
                cwd=str(self._tmpdir),
                env=run_container_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
            )

            # Live logging of Docker build
            assert proc.stdout
            with proc.stdout:
                for line in iter(proc.stdout.readline, ""):
                    LOG.info(line.rstrip("\r\n"))
        except Exception as e:  # pragma: no cover
            LOG.exception(f"Failed to start Worker agent Docker container: {e}")
            _handle_subprocess_error(e)
            raise
        else:
            exit_code = proc.wait(timeout=60)
            assert exit_code == 0, f"Process failed with exit code {exit_code}"

        # Grab the container ID from --cidfile
        try:
            self._container_id = subprocess.check_output(
                args=["cat", ".container_id"],
                cwd=str(self._tmpdir),
                text=True,
                encoding="utf-8",
                timeout=1,
            ).rstrip("\r\n")
        except Exception as e:  # pragma: no cover
            LOG.exception(f"Failed to get Docker container ID: {e}")
            _handle_subprocess_error(e)
            raise
        else:
            LOG.info(f"Started Docker container {self._container_id}")

    def stop(self) -> None:
        assert (
            self._container_id
        ), "Cannot stop Docker container: Container ID is not set. Has the Docker container been started yet?"

        LOG.info(f"Terminating Worker agent process in Docker container {self._container_id}")
        try:
            self.send_command(f"pkill --signal term -f {self.configuration.user}")
        except Exception as e:  # pragma: no cover
            LOG.exception(f"Failed to terminate Worker agent process: {e}")
            raise
        else:
            LOG.info("Worker agent process terminated")

        LOG.info(f"Stopping Docker container {self._container_id}")
        try:
            subprocess.check_output(
                args=["docker", "container", "stop", self._container_id],
                cwd=str(self._tmpdir),
                text=True,
                encoding="utf-8",
                timeout=30,
            )
        except Exception as e:  # pragma: noc over
            LOG.exception(f"Failed to stop Docker container {self._container_id}: {e}")
            _handle_subprocess_error(e)
            raise
        else:
            LOG.info(f"Stopped Docker container {self._container_id}")
            self._container_id = None

    def send_command(self, command: str, *, quiet: bool = False) -> CommandResult:
        assert (
            self._container_id
        ), "Container ID not set. Has the Docker container been started yet?"

        if not quiet:  # pragma: no cover
            LOG.info(f"Sending command '{command}' to Docker container {self._container_id}")
        try:
            result = subprocess.run(
                args=[
                    "docker",
                    "exec",
                    self._container_id,
                    "/bin/bash",
                    "-euo",
                    "pipefail",
                    "-c",
                    command,
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
            )
        except Exception as e:
            if not quiet:  # pragma: no cover
                LOG.exception(f"Failed to run command: {e}")
                _handle_subprocess_error(e)
            raise
        else:
            return CommandResult(
                exit_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )

    @property
    def worker_id(self) -> str:
        cmd_result: Optional[CommandResult] = None

        def got_worker_id() -> bool:
            nonlocal cmd_result
            try:
                cmd_result = self.send_command(
                    "cat /var/lib/deadline/worker.json | jq -r '.worker_id'",
                    quiet=True,
                )
            except subprocess.CalledProcessError as e:
                LOG.warning(f"Worker ID retrieval failed: {e}")
                return False
            else:
                return cmd_result.exit_code == 0

        wait_for(
            description="retrieval of worker ID from /var/lib/deadline/worker.json",
            predicate=got_worker_id,
            interval_s=10,
            max_retries=6,
        )

        assert isinstance(cmd_result, CommandResult)
        cmd_result = cast(CommandResult, cmd_result)
        assert cmd_result.exit_code == 0, f"Failed to get Worker ID: {cmd_result}"

        worker_id = cmd_result.stdout.rstrip("\r\n")
        assert re.match(
            r"^worker-[0-9a-f]{32}$", worker_id
        ), f"Got nonvalid Worker ID from command stdout: {cmd_result}"

        return worker_id

    @property
    def container_id(self) -> str | None:
        return self._container_id


def _handle_subprocess_error(e: Any) -> None:  # pragma: no cover
    if hasattr(e, "stdout"):
        LOG.error(f"Command stdout: {e.stdout}")
    if hasattr(e, "stderr"):
        LOG.error(f"Command stderr: {e.stderr}")
