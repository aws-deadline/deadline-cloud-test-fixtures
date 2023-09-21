# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import json
import os
import pathlib
import re
import subprocess
from typing import Any, Generator
from unittest.mock import ANY, MagicMock, call, mock_open, patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_ec2, mock_iam, mock_s3, mock_ssm

from deadline_test_fixtures.deadline import worker as mod
from deadline_test_fixtures import (
    CodeArtifactRepositoryInfo,
    CommandResult,
    DeadlineWorkerConfiguration,
    DockerContainerWorker,
    EC2InstanceWorker,
    PipInstall,
    S3Object,
    ServiceModel,
)


@pytest.fixture(autouse=True)
def moto_mocks() -> Generator[None, None, None]:
    with mock_ec2(), mock_iam(), mock_s3(), mock_ssm():
        yield


@pytest.fixture(autouse=True)
def mock_sleep() -> Generator[None, None, None]:
    # We don't want to sleep in unit tests
    with patch.object(mod.time, "sleep"):
        yield


@pytest.fixture(autouse=True)
def wait_for_shim() -> Generator[None, None, None]:
    import sys
    from deadline_test_fixtures.util import wait_for

    # Force the wait_for to have a short interval for unit tests
    def wait_for_shim(*args, **kwargs):
        kwargs.pop("interval_s", None)
        kwargs.pop("max_retries", None)
        wait_for(*args, **kwargs, interval_s=sys.float_info.epsilon, max_retries=None)

    with patch.object(mod, "wait_for", wait_for_shim):
        yield


@pytest.fixture
def region(boto_config: dict[str, str]) -> str:
    return boto_config["AWS_DEFAULT_REGION"]


@pytest.fixture
def deadline_client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def worker_config(region: str) -> DeadlineWorkerConfiguration:
    return DeadlineWorkerConfiguration(
        farm_id="farm-123",
        fleet_id="fleet-123",
        region=region,
        user="test-user",
        group="test-group",
        allow_shutdown=False,
        worker_agent_install=PipInstall(
            requirement_specifiers=["deadline-cloud-worker-agent"],
            codeartifact=CodeArtifactRepositoryInfo(
                region=region,
                domain="test-domain",
                domain_owner="123456789123",
                repository="test-repository",
            ),
        ),
        file_mappings=[
            ("/tmp/file1.txt", "/home/test-user/file1.txt"),
            ("/packages/manifest.json", "/tmp/manifest.json"),
            ("/aws/models/deadline.json", "/tmp/deadline.json"),
        ],
        service_model=ServiceModel(
            file_path="/tmp/deadline/1234-12-12/service-2.json",
            api_version="1234-12-12",
            service_name="deadline",
        ),
    )


class TestEC2InstanceWorker:
    @staticmethod
    def describe_instance(instance_id: str) -> Any:
        ec2_client = boto3.client("ec2")
        response = ec2_client.describe_instances(InstanceIds=[instance_id])

        reservations = response["Reservations"]
        assert len(reservations) == 1

        instances = reservations[0]["Instances"]
        assert len(instances) == 1

        return instances[0]

    @pytest.fixture
    def vpc_id(self) -> str:
        return boto3.client("ec2").create_vpc(CidrBlock="10.0.0.0/28")["Vpc"]["VpcId"]

    @pytest.fixture
    def subnet_id(self, vpc_id: str) -> str:
        return boto3.client("ec2").create_subnet(
            VpcId=vpc_id,
            CidrBlock="10.0.0.0/28",
        )[
            "Subnet"
        ]["SubnetId"]

    @pytest.fixture
    def security_group_id(self, vpc_id: str) -> str:
        return boto3.client("ec2").create_security_group(
            VpcId=vpc_id,
            Description="Testing",
            GroupName="TestSG",
        )["GroupId"]

    @pytest.fixture
    def instance_profile(self) -> Any:
        return boto3.client("iam").create_instance_profile(InstanceProfileName="instance-profile")[
            "InstanceProfile"
        ]

    @pytest.fixture
    def instance_profile_name(self, instance_profile: Any) -> str:
        return instance_profile["InstanceProfileName"]

    @pytest.fixture
    def bootstrap_bucket_name(self, region: str) -> str:
        name = "bootstrap-bucket"
        kwargs: dict[str, Any] = {"Bucket": name}
        if region != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
        boto3.client("s3").create_bucket(**kwargs)
        return name

    @pytest.fixture
    def worker(
        self,
        deadline_client: MagicMock,
        worker_config: DeadlineWorkerConfiguration,
        subnet_id: str,
        security_group_id: str,
        instance_profile_name: str,
        bootstrap_bucket_name: str,
    ) -> EC2InstanceWorker:
        return EC2InstanceWorker(
            subnet_id=subnet_id,
            security_group_id=security_group_id,
            instance_profile_name=instance_profile_name,
            bootstrap_bucket_name=bootstrap_bucket_name,
            s3_client=boto3.client("s3"),
            ec2_client=boto3.client("ec2"),
            ssm_client=boto3.client("ssm"),
            deadline_client=deadline_client,
            configuration=worker_config,
        )

    @patch.object(mod, "open", mock_open(read_data="mock data".encode()))
    def test_start(self, worker: EC2InstanceWorker) -> None:
        # GIVEN
        s3_files = [
            ("s3://bucket/key", "/tmp/key"),
            ("s3://bucket/tmp/file", "/tmp/file"),
        ]
        with (
            patch.object(worker, "_stage_s3_bucket", return_value=s3_files) as mock_stage_s3_bucket,
            patch.object(worker, "_launch_instance") as mock_launch_instance,
            patch.object(worker, "_start_worker_agent") as mock_start_worker_agent,
        ):
            # WHEN
            worker.start()

        # THEN
        # Detailed testing for each of these is done in dedicated test methods
        mock_stage_s3_bucket.assert_called_once()
        mock_launch_instance.assert_called_once_with(s3_files=s3_files)
        mock_start_worker_agent.assert_called_once()

    def test_stage_s3_bucket(
        self,
        worker: EC2InstanceWorker,
        worker_config: DeadlineWorkerConfiguration,
        bootstrap_bucket_name: str,
    ) -> None:
        # GIVEN
        # We don't want to actually match real files, just limit src paths to absolute paths
        with (
            patch.object(mod.glob, "glob", lambda path: [path]),
            patch.object(mod, "open", mock_open(read_data="mock data".encode())),
        ):
            # WHEN
            s3_files = worker._stage_s3_bucket()

        # THEN
        # Verify mappings are correct
        assert s3_files is not None and worker_config.file_mappings is not None
        assert len(s3_files) == len(worker_config.file_mappings)
        for src, dst in worker_config.file_mappings:
            assert (f"s3://{bootstrap_bucket_name}/worker/{os.path.basename(src)}", dst) in s3_files

        # Verify files are uploaded to S3
        s3_client = boto3.client("s3")
        for s3_uri, _ in s3_files:
            s3_obj = S3Object.from_uri(s3_uri)
            s3_client.head_object(Bucket=s3_obj.bucket, Key=s3_obj.key)

    def test_launch_instance(
        self,
        worker: EC2InstanceWorker,
        vpc_id: str,
        subnet_id: str,
        security_group_id: str,
        instance_profile: Any,
    ) -> None:
        # WHEN
        worker._launch_instance()

        # THEN
        assert worker.instance_id is not None

        instance = TestEC2InstanceWorker.describe_instance(worker.instance_id)
        assert instance["ImageId"] == worker.ami_id
        assert instance["State"]["Name"] == "running"
        assert instance["SubnetId"] == subnet_id
        assert instance["VpcId"] == vpc_id
        assert instance["IamInstanceProfile"]["Arn"] == instance_profile["Arn"]
        assert len(instance["SecurityGroups"]) == 1
        assert instance["SecurityGroups"][0]["GroupId"] == security_group_id

    @pytest.mark.skip(
        "There's nothing to test in this method currently since it's just sending SSM commands"
    )
    def test_start_worker_agent(self) -> None:
        pass

    def test_stop(self, worker: EC2InstanceWorker) -> None:
        # GIVEN
        worker.start()
        instance_id = worker.instance_id
        assert instance_id is not None

        instance = TestEC2InstanceWorker.describe_instance(instance_id)
        assert instance["State"]["Name"] == "running"

        # WHEN
        worker.stop()

        # THEN
        instance = TestEC2InstanceWorker.describe_instance(instance_id)
        assert instance["State"]["Name"] == "terminated"
        assert worker.instance_id is None

    class TestSendCommand:
        def test_sends_command(self, worker: EC2InstanceWorker) -> None:
            # GIVEN
            cmd = 'echo "Hello world"'
            worker.start()

            # WHEN
            with patch.object(
                worker.ssm_client, "send_command", wraps=worker.ssm_client.send_command
            ) as send_command_spy:
                worker.send_command(cmd)

            # THEN
            send_command_spy.assert_called_once_with(
                InstanceIds=[worker.instance_id],
                DocumentName="AWS-RunShellScript",
                Parameters={"commands": [cmd]},
            )

        def test_retries_when_instance_not_ready(self, worker: EC2InstanceWorker) -> None:
            # GIVEN
            cmd = 'echo "Hello world"'
            worker.start()
            real_send_command = worker.ssm_client.send_command

            call_count = 0

            def side_effect(*args, **kwargs):
                nonlocal call_count
                if call_count < 1:
                    call_count += 1
                    raise ClientError({"Error": {"Code": "InvalidInstanceId"}}, "SendCommand")
                else:
                    return real_send_command(*args, **kwargs)

            # WHEN
            with patch.object(
                worker.ssm_client, "send_command", side_effect=side_effect
            ) as mock_send_command:
                worker.send_command(cmd)

            # THEN
            mock_send_command.assert_has_calls(
                [
                    call(
                        InstanceIds=[worker.instance_id],
                        DocumentName="AWS-RunShellScript",
                        Parameters={"commands": [cmd]},
                    )
                ]
                * 2
            )

        def test_raises_any_other_error(self, worker: EC2InstanceWorker) -> None:
            # GIVEN
            cmd = 'echo "Hello world"'
            worker.start()
            err = ClientError({"Error": {"Code": "SomethingWentWrong"}}, "SendCommand")

            # WHEN
            with pytest.raises(ClientError) as raised_err:
                with patch.object(
                    worker.ssm_client, "send_command", side_effect=err
                ) as mock_send_command:
                    worker.send_command(cmd)

            # THEN
            assert raised_err.value is err
            mock_send_command.assert_called_once_with(
                InstanceIds=[worker.instance_id],
                DocumentName="AWS-RunShellScript",
                Parameters={"commands": [cmd]},
            )

    @pytest.mark.parametrize(
        "worker_id",
        [
            "worker-7c3377ec9eba444bb51cc7da18463081",
            "worker-7c3377ec9eba444bb51cc7da18463081\n",
            "worker-7c3377ec9eba444bb51cc7da18463081\r\n",
        ],
    )
    def test_worker_id(self, worker_id: str, worker: EC2InstanceWorker) -> None:
        # GIVEN
        with patch.object(
            worker, "send_command", return_value=CommandResult(exit_code=0, stdout=worker_id)
        ):
            # WHEN
            result = worker.worker_id

        # THEN
        assert result == worker_id.rstrip("\n\r")

    def test_ami_id(self, worker: EC2InstanceWorker) -> None:
        # WHEN
        ami_id = worker.ami_id

        # THEN
        assert re.match(r"^ami-[0-9a-f]{17}$", ami_id)


class TestDockerContainerWorker:
    @pytest.fixture
    def worker(self, worker_config: DeadlineWorkerConfiguration) -> DockerContainerWorker:
        return DockerContainerWorker(configuration=worker_config)

    def test_start(
        self,
        worker: DockerContainerWorker,
        worker_config: DeadlineWorkerConfiguration,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # GIVEN
        caplog.set_level("INFO")

        # file_mappings
        tmpdir = os.path.join(os.sep, "tmp")

        # subprocess.Popen("./run_container.sh")
        run_container_stdout_lines = ["line1", "line2", ""]
        mock_proc = MagicMock()
        mock_proc.stdout.readline.side_effect = run_container_stdout_lines
        mock_proc.wait.return_value = 0

        # subprocess.check_output("cat .container_id")
        container_id = "798914422427460f83827544bfca1d83"

        with (
            patch.object(mod, "shutil") as mock_shutil,
            patch.object(mod.tempfile, "mkdtemp", return_value=tmpdir),
            patch.object(mod.os, "makedirs") as mock_makedirs,
            patch.object(mod.subprocess, "Popen") as mock_Popen,
            patch.object(mod.subprocess, "check_output") as mock_check_output,
        ):
            mock_Popen.return_value = mock_proc
            mock_check_output.return_value = container_id

            # WHEN
            worker.start()

        # THEN
        mock_shutil.copytree.assert_called_once_with(ANY, tmpdir, dirs_exist_ok=True)

        # Verify file_mappings dir is staged
        file_mappings_dir = os.path.join(tmpdir, "file_mappings")
        mock_makedirs.assert_called_once_with(file_mappings_dir)
        assert worker_config.file_mappings
        for src, _ in worker_config.file_mappings:
            mock_shutil.copyfile.assert_any_call(
                src, os.path.join(file_mappings_dir, os.path.basename(src))
            )

        # Verify subprocess.Popen("./run_container.sh")
        _, popen_kwargs = mock_Popen.call_args
        assert popen_kwargs["args"] == "./run_container.sh"
        assert popen_kwargs["cwd"] == ANY
        assert popen_kwargs["stdout"] == subprocess.PIPE
        assert popen_kwargs["stderr"] == subprocess.STDOUT
        assert popen_kwargs["text"] is True
        assert popen_kwargs["encoding"] == "utf-8"
        expected_env = {
            "FILE_MAPPINGS": ANY,
            "AGENT_USER": worker_config.user,
            "SHARED_GROUP": worker_config.group,
            "JOB_USER": "jobuser",
            "CONFIGURE_WORKER_AGENT_CMD": ANY,
        }
        actual_env = popen_kwargs["env"]
        for expected_key, expected_value in expected_env.items():
            assert expected_key in actual_env
            assert actual_env[expected_key] == expected_value
        assert all(line in caplog.text for line in run_container_stdout_lines)
        mock_proc.wait.assert_called_once()

        # Verify FILE_MAPPINGS env var is generated correctly
        actual_file_mappings = json.loads(actual_env["FILE_MAPPINGS"])
        for src, dst in worker_config.file_mappings:
            docker_src = f"/file_mappings/{os.path.basename(src)}"
            assert docker_src in actual_file_mappings
            assert actual_file_mappings[docker_src] == dst

        # Verify subprocess.check_output("cat .container_id")
        _, check_output_kwargs = mock_check_output.call_args
        assert check_output_kwargs["args"] == ["cat", ".container_id"]
        assert check_output_kwargs["cwd"] == ANY
        assert check_output_kwargs["text"] is True
        assert check_output_kwargs["encoding"] == "utf-8"
        assert check_output_kwargs["timeout"] == 1
        assert worker.container_id == container_id

    def test_stop(
        self, worker: DockerContainerWorker, worker_config: DeadlineWorkerConfiguration
    ) -> None:
        # GIVEN
        container_id = "container-id"
        worker._container_id = container_id
        worker._tmpdir = pathlib.Path("/tmp")

        with (
            patch.object(worker, "send_command") as mock_send_command,
            patch.object(mod.subprocess, "check_output") as mock_check_output,
        ):
            # WHEN
            worker.stop()

        # THEN
        assert worker.container_id is None
        mock_send_command.assert_called_once_with(f"pkill --signal term -f {worker_config.user}")
        mock_check_output.assert_called_once_with(
            args=["docker", "container", "stop", container_id],
            cwd=ANY,
            text=True,
            encoding="utf-8",
            timeout=30,
        )

    def test_send_command(self, worker: DockerContainerWorker) -> None:
        # GIVEN
        worker._container_id = "container-id"
        cmd = 'echo "Hello world"'
        mock_run_result = MagicMock()
        mock_run_result.returncode = 0
        mock_run_result.stdout = "Hello world"
        mock_run_result.stderr = None

        with patch.object(mod.subprocess, "run", return_value=mock_run_result) as mock_run:
            # WHEN
            result = worker.send_command(cmd)

        # THEN
        assert result.exit_code == 0
        assert result.stdout == "Hello world"
        assert result.stderr is None
        mock_run.assert_called_once_with(
            args=[
                "docker",
                "exec",
                worker.container_id,
                "/bin/bash",
                "-euo",
                "pipefail",
                "-c",
                cmd,
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
        )

    def test_worker_id(self, worker: DockerContainerWorker) -> None:
        # GIVEN
        worker._container_id = "container-id"
        worker_id = "worker-3ff2c8b6c6a5452f8f7b85cd45b80ba3"
        send_command_result = CommandResult(0, f"{worker_id}\r\n")

        with patch.object(worker, "send_command", return_value=send_command_result):
            # WHEN
            result = worker.worker_id

        # THEN
        assert result == worker_id
