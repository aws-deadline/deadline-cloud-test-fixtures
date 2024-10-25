# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import botocore
import botocore.client
import botocore.loaders
import boto3
import glob
import json
import logging
import os
import pathlib
import posixpath
import pytest
import tempfile
from contextlib import ExitStack, contextmanager
from dataclasses import InitVar, dataclass, field, fields, MISSING
from typing import Any, Generator, Type, TypeVar

from .deadline.client import DeadlineClient
from .deadline.resources import (
    Farm,
    Fleet,
    Queue,
    QueueFleetAssociation,
)
from .deadline.worker import (
    DeadlineWorker,
    DeadlineWorkerConfiguration,
    DockerContainerWorker,
    PipInstall,
    PosixInstanceBuildWorker,
    WindowsInstanceBuildWorker,
    EC2InstanceWorker,
)
from .models import (
    CodeArtifactRepositoryInfo,
    JobAttachmentSettings,
    JobRunAsUser,
    PosixSessionUser,
    ServiceModel,
    S3Object,
    OperatingSystem,
    WindowsSessionUser,
)
from .cloudformation import WorkerBootstrapStack
from .job_attachment_manager import JobAttachmentManager
from .util import call_api

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class BootstrapResources:
    bootstrap_bucket_name: str
    worker_role_arn: str
    session_role_arn: str | None = None
    worker_instance_profile_name: str | None = None

    job_attachments: JobAttachmentSettings | None = field(init=False, default=None)
    job_attachments_bucket_name: str | None = None
    job_attachments_root_prefix: str | None = None

    windows_run_as_user: str | None = None
    windows_run_as_user_secret_arn: str | None = None
    posix_run_as_user: str | None = None
    posix_run_as_user_group: str | None = None

    job_run_as_user: JobRunAsUser = field(
        default_factory=lambda: JobRunAsUser(
            posix=PosixSessionUser("", ""),
            runAs="WORKER_AGENT_USER",
            windows=WindowsSessionUser("", ""),
        )
    )

    def __post_init__(self) -> None:
        if self.job_attachments_bucket_name or self.job_attachments_root_prefix:
            assert (
                self.job_attachments_bucket_name and self.job_attachments_root_prefix
            ), "Cannot provide partial Job Attachments settings, both bucket name and root prefix are required"
            object.__setattr__(
                self,
                "job_attachments",
                JobAttachmentSettings(
                    bucket_name=self.job_attachments_bucket_name,
                    root_prefix=self.job_attachments_root_prefix,
                ),
            )
        if (
            self.windows_run_as_user
            or self.windows_run_as_user_secret_arn
            or self.posix_run_as_user
            or self.posix_run_as_user_group
        ):
            assert (
                self.windows_run_as_user and self.windows_run_as_user_secret_arn
            ), "Cannot provide partial Windows run as user settings, both user name and secret arn are required"
            assert (
                self.posix_run_as_user and self.posix_run_as_user_group
            ), "Cannot provide partial Posix run as user settings, both user name and user group are required"
            object.__setattr__(
                self,
                "job_run_as_user",
                JobRunAsUser(
                    posix=PosixSessionUser(self.posix_run_as_user, self.posix_run_as_user_group),
                    runAs="QUEUE_CONFIGURED_USER",
                    windows=WindowsSessionUser(
                        self.windows_run_as_user, self.windows_run_as_user_secret_arn
                    ),
                ),
            )


@dataclass(frozen=True)
class DeadlineResources:
    farm: Farm = field(init=False)
    queue: Queue = field(init=False)
    fleet: Fleet = field(init=False)

    farm_id: InitVar[str]
    queue_id: InitVar[str]
    fleet_id: InitVar[str]

    farm_kms_key_id: str | None = None
    job_attachments_bucket: str | None = None

    def __post_init__(
        self,
        farm_id: str,
        queue_id: str,
        fleet_id: str,
    ) -> None:
        object.__setattr__(self, "farm", Farm(id=farm_id))
        object.__setattr__(self, "queue", Queue(id=queue_id, farm=self.farm))
        object.__setattr__(self, "fleet", Fleet(id=fleet_id, farm=self.farm))


@pytest.fixture(scope="session")
def deadline_client(
    # Explicitly request fixture since pytest.mark.usefixtures doesn't work on fixtures
    install_service_model: str,
) -> DeadlineClient:
    endpoint_url = os.getenv("DEADLINE_ENDPOINT")
    if endpoint_url:
        LOG.info(f"Using AWS Deadline Cloud endpoint: {endpoint_url}")

    session = boto3.Session()
    session._loader.search_paths.extend([install_service_model])

    return DeadlineClient(session.client("deadline", endpoint_url=endpoint_url))


@pytest.fixture(scope="session")
def codeartifact() -> CodeArtifactRepositoryInfo:
    """
    Gets the information for the CodeArtifact repository to use for Python dependencies.

    Environment Variables:
        CODEARTIFACT_REGION: The region the CodeArtifact repository is in
        CODEARTIFACT_DOMAIN: The domain of the CodeArtifact repository
        CODEARTIFACT_ACCOUNT_ID: The AWS account ID which owns the domain
        CODEARTIFACT_REPOSITORY: The name of the CodeArtifact repository

    Returns:
        CodeArtifactRepositoryInfo: Info about the CodeArtifact repository
    """
    return CodeArtifactRepositoryInfo(
        region=os.environ["CODEARTIFACT_REGION"],
        domain=os.environ["CODEARTIFACT_DOMAIN"],
        domain_owner=os.environ["CODEARTIFACT_ACCOUNT_ID"],
        repository=os.environ["CODEARTIFACT_REPOSITORY"],
    )


@pytest.fixture(scope="session")
def region() -> str:
    return os.getenv(
        "REGION", os.getenv("WORKER_REGION", os.getenv("AWS_DEFAULT_REGION", "us-west-2"))
    )


@pytest.fixture(scope="session")
def service_model() -> Generator[ServiceModel, None, None]:
    service_model_s3_uri = os.getenv("DEADLINE_SERVICE_MODEL_S3_URI")
    local_model_path = os.getenv("LOCAL_MODEL_PATH")

    assert not (
        service_model_s3_uri and local_model_path
    ), "Cannot provide both DEADLINE_SERVICE_MODEL_S3_URI and LOCAL_MODEL_PATH"

    if service_model_s3_uri:
        LOG.info(f"Using Deadline model from S3: {service_model_s3_uri}")

        LOG.info(f"Downloading {service_model_s3_uri}")
        s3_obj = S3Object.from_uri(service_model_s3_uri)
        s3_client = boto3.client("s3")

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = os.path.join(tmpdir, "service-2.json")
            call_api(
                description=f"Downloading {service_model_s3_uri}",
                fn=lambda: s3_client.download_file(s3_obj.bucket, s3_obj.key, json_path),
            )
            yield ServiceModel.from_json_file(json_path)
    else:
        if not local_model_path:
            local_model_path = _find_latest_service_model_file("deadline")
        LOG.info(f"Using service model at: {local_model_path}")
        if local_model_path.endswith(".json"):
            yield ServiceModel.from_json_file(local_model_path)
        elif local_model_path.endswith(".json.gz"):
            yield ServiceModel.from_json_gz_file(local_model_path)
        else:
            raise RuntimeError(
                f"Unsupported service model file format (must be .json or .json.gz): {local_model_path}"
            )


@pytest.fixture(scope="session")
def install_service_model(service_model: ServiceModel, region: str) -> Generator[str, None, None]:
    LOG.info("Installing service model and configuring boto to use it for API calls")
    with service_model.install(region) as service_model_install:
        LOG.info(f"Installed service model to {service_model_install}")
        yield service_model_install


@pytest.fixture(scope="session")
def bootstrap_resources(request: pytest.FixtureRequest) -> BootstrapResources:
    """
    Gets Bootstrap resources required for running tests.

    Environment Variables:
        SERVICE_ACCOUNT_ID: ID of the AWS account to deploy the bootstrap stack into.
            This option is ignored if BYO_BOOTSTRAP is set to "true"
        CREDENTIAL_VENDING_PRINCIPAL: The credential vending service principal to use.
            Defaults to credentials.deadline.amazonaws.com
            This option is ignored if BYO_BOOTSTRAP is set to "true"
        BYO_BOOTSTRAP: Whether the bootstrap stack deployment should be skipped.
            If this is set to "true", environment values must be specified to fill the resources.
        <BOOTSTRAP_RESOURCE>: Corresponds to an field in the BootstrapResources class with an uppercase name.
            e.g. WORKER_ROLE_ARN -> BootstrapResources.worker_role_arn

    Returns:
        BootstrapResources: The bootstrap resources used for tests
    """

    if os.environ.get("BYO_BOOTSTRAP", "false").lower() == "true":
        kwargs: dict[str, Any] = {}

        all_fields = fields(BootstrapResources)
        for f in all_fields:
            env_var = f.name.upper()
            if env_var in os.environ:
                kwargs[f.name] = os.environ[env_var]

        required_fields = [f for f in all_fields if (MISSING == f.default == f.default_factory)]
        assert all([rf.name in kwargs for rf in required_fields]), (
            "Not all bootstrap resources have been fulfilled via environment variables. Expected "
            + f"values for {[f.name.upper() for f in required_fields]}, but got \n{json.dumps(kwargs, sort_keys=True, indent=4)}"
        )
        LOG.info(
            f"All bootstrap resources have been fulfilled via environment variables. Using \n{json.dumps(kwargs, sort_keys=True, indent=4)}"
        )
        return BootstrapResources(**kwargs)
    else:
        account = os.environ["SERVICE_ACCOUNT_ID"]
        codeartifact: CodeArtifactRepositoryInfo = request.getfixturevalue("codeartifact")
        credential_vending_service_principal = os.getenv(
            "CREDENTIAL_VENDING_PRINCIPAL",
            "credentials.deadline.amazonaws.com",
        )

        stack_name = "DeadlineScaffoldingWorkerBootstrapStack"
        LOG.info(f"Deploying bootstrap stack {stack_name}")
        stack = WorkerBootstrapStack(
            name=stack_name,
            codeartifact=codeartifact,
            account=account,
            credential_vending_service_principal=credential_vending_service_principal,
        )
        stack.deploy(cfn_client=boto3.client("cloudformation"))

        return BootstrapResources(
            bootstrap_bucket_name=stack.bootstrap_bucket.physical_name,
            worker_role_arn=stack.worker_role.format_arn(account=account),
            session_role_arn=stack.session_role.format_arn(account=account),
            worker_instance_profile_name=stack.worker_instance_profile.physical_name,
            job_attachments_bucket_name=stack.job_attachments_bucket.physical_name,
            job_attachments_root_prefix="root",
        )


@pytest.fixture(scope="session")
def deadline_resources(
    request: pytest.FixtureRequest,
    deadline_client: DeadlineClient,
) -> Generator[DeadlineResources, None, None]:
    """
    Gets Deadline resources required for running tests.

    Environment Variables:
        BYO_DEADLINE: Whether the Deadline resource deployment should be skipped.
            If this is set to "true", environment values must be specified to fill the resources.
        <DEADLINE_RESOURCE>: Corresponds to an field in the DeadlineResources class with an uppercase name.
            e.g. FARM_ID -> DeadlineResources.farm_id

    Returns:
        DeadlineResources: The Deadline resources used for tests
    """
    if os.getenv("BYO_DEADLINE", "false").lower() == "true":
        kwargs: dict[str, Any] = {}
        resource_env_vars: list[str] = [
            "FARM_ID",
            "FLEET_ID",
            "QUEUE_ID",
        ]

        for env_var in resource_env_vars:
            if env_var in os.environ:
                kwargs[env_var.lower()] = os.environ[env_var]

        yield DeadlineResources(**kwargs)
    else:
        LOG.info("Deploying Deadline resources")
        bootstrap_resources: BootstrapResources = request.getfixturevalue("bootstrap_resources")

        # Define a context manager for robust cleanup of resources
        T = TypeVar("T", Farm, Fleet, Queue, QueueFleetAssociation)

        @contextmanager
        def deletable(resource: T) -> Generator[T, None, None]:
            try:
                yield resource
            finally:
                resource.delete(client=deadline_client)

        with ExitStack() as context_stack:
            farm = context_stack.enter_context(
                deletable(
                    Farm.create(
                        client=deadline_client,
                        display_name="test-scaffolding-farm",
                    )
                )
            )
            queue = context_stack.enter_context(
                deletable(
                    Queue.create(
                        client=deadline_client,
                        display_name="test-scaffolding-queue",
                        farm=farm,
                        job_attachments=bootstrap_resources.job_attachments,
                        role_arn=bootstrap_resources.session_role_arn,
                        job_run_as_user=bootstrap_resources.job_run_as_user,
                    )
                )
            )
            fleet = context_stack.enter_context(
                deletable(
                    Fleet.create(
                        client=deadline_client,
                        display_name="test-scaffolding-fleet",
                        farm=farm,
                        configuration={
                            "customerManaged": {
                                "mode": "NO_SCALING",
                                "workerCapabilities": {
                                    "vCpuCount": {"min": 1},
                                    "memoryMiB": {"min": 1024},
                                    "osFamily": "linux",
                                    "cpuArchitectureType": "x86_64",
                                },
                            },
                        },
                        max_worker_count=1,
                        role_arn=bootstrap_resources.worker_role_arn,
                    )
                )
            )
            context_stack.enter_context(
                deletable(
                    QueueFleetAssociation.create(
                        client=deadline_client,
                        farm=farm,
                        queue=queue,
                        fleet=fleet,
                    )
                )
            )

            yield DeadlineResources(
                farm_id=farm.id,
                queue_id=queue.id,
                fleet_id=fleet.id,
                job_attachments_bucket=(
                    bootstrap_resources.job_attachments.bucket_name
                    if bootstrap_resources.job_attachments
                    else None
                ),
            )


@pytest.fixture(scope="session")
def worker_config(
    deadline_resources: DeadlineResources,
    codeartifact: CodeArtifactRepositoryInfo,
    service_model: ServiceModel,
    region: str,
    operating_system: OperatingSystem,
) -> Generator[DeadlineWorkerConfiguration, None, None]:
    """
    Builds the configuration for a DeadlineWorker.

    Environment Variables:
        OPENJD_SESSIONS_WHL_PATH: Path to the openjd-sessions wheel file to use.
        WORKER_POSIX_USER: The POSIX user to configure the worker for
            Defaults to "deadline-worker"
        WORKER_POSIX_SHARED_GROUP: The shared POSIX group to configure the worker user and job user with
            Defaults to "shared-group"
        WORKER_AGENT_WHL_PATH: Path to the Worker agent wheel file to use.
        WORKER_AGENT_REQUIREMENT_SPECIFIER: PEP 508 requirement specifier for the Worker agent package.
            If WORKER_AGENT_WHL_PATH is provided, this option is ignored.
        LOCAL_MODEL_PATH: Path to a local Deadline model file to use for API calls.
            If DEADLINE_SERVICE_MODEL_S3_URI was provided, this option is ignored.

    Returns:
        DeadlineWorkerConfiguration: Configuration for use by DeadlineWorker.
    """
    file_mappings: list[tuple[str, str]] = []

    # Deprecated environment variable
    if os.getenv("WORKER_REGION") is not None:
        LOG.warning(
            "DEPRECATED: The environment variable WORKER_REGION is no longer supported. Please use REGION instead."
        )

    pip_install_requirement_specifiers: list[str] = []

    # Prepare the Worker agent Python package
    worker_agent_whl_path = os.getenv("WORKER_AGENT_WHL_PATH")
    if worker_agent_whl_path:
        LOG.info(f"Using Worker agent whl file: {worker_agent_whl_path}")
        resolved_whl_paths = glob.glob(worker_agent_whl_path)
        assert (
            len(resolved_whl_paths) == 1
        ), f"Expected exactly one Worker agent whl path, but got {resolved_whl_paths} (from pattern {worker_agent_whl_path})"
        resolved_whl_path = resolved_whl_paths[0]

        if operating_system.is_amazon_linux():
            dest_path = posixpath.join("/tmp", os.path.basename(resolved_whl_path))
        elif operating_system.is_windows():
            dest_path = posixpath.join(
                "C:\\Windows\\System32\\Config\\systemprofile\\AppData\\Local\\Temp",
                os.path.basename(resolved_whl_path),
            )
        else:
            raise NotImplementedError(f"Unsupported operating system: {operating_system.name}")
        file_mappings = [(resolved_whl_path, dest_path)]

        LOG.info(
            f"The worker agent whl file will be copied to {dest_path} on the Worker environment"
        )
        pip_install_requirement_specifiers.append(dest_path)
    else:
        worker_agent_requirement_specifier = os.getenv(
            "WORKER_AGENT_REQUIREMENT_SPECIFIER",
            "deadline-cloud-worker-agent",
        )
        LOG.info(f"Using Worker agent package {worker_agent_requirement_specifier}")
        pip_install_requirement_specifiers.append(worker_agent_requirement_specifier)

    openjd_sessions_whl_path = os.getenv("OPENJD_SESSIONS_WHL_PATH")
    if openjd_sessions_whl_path:
        LOG.info(f"Using openjd-sessions agent whl file: {openjd_sessions_whl_path}")
        resolved_whl_paths = glob.glob(openjd_sessions_whl_path)
        assert (
            len(resolved_whl_paths) == 1
        ), f"Expected exactly one openjd-sessions whl path, but got {resolved_whl_paths} (from pattern {openjd_sessions_whl_path})"
        resolved_whl_path = resolved_whl_paths[0]

        if operating_system.is_amazon_linux():
            dest_path = posixpath.join("/tmp", os.path.basename(resolved_whl_path))
        elif operating_system.is_windows():
            dest_path = posixpath.join(
                "C:\\Windows\\System32\\Config\\systemprofile\\AppData\\Local\\Temp",
                os.path.basename(resolved_whl_path),
            )
        else:
            raise NotImplementedError(f"Unsupported operating system: {operating_system.name}")
        file_mappings.append((resolved_whl_path, dest_path))
        LOG.info(
            f"The openjd-sessions whl file will be copied to {dest_path} on the Worker environment"
        )
        pip_install_requirement_specifiers.append(dest_path)

    # Path map the service model
    with tempfile.TemporaryDirectory() as tmpdir:
        src_path = pathlib.Path(tmpdir) / f"{service_model.service_name}-service-2.json"

        LOG.info(f"Staging service model to {src_path} for uploading to S3")
        with src_path.open(mode="w") as f:
            json.dump(service_model.model, f)

        if operating_system.is_amazon_linux():
            dst_path = posixpath.join("/tmp", src_path.name)
        elif operating_system.is_windows():
            dst_path = posixpath.join(
                "C:\\Windows\\System32\\Config\\systemprofile\\AppData\\Local\\Temp", src_path.name
            )
        else:
            raise NotImplementedError(f"Unsupported operating system: {operating_system.name}")
        LOG.info(f"The service model will be copied to {dst_path} on the Worker environment")
        file_mappings.append((str(src_path), dst_path))

        yield DeadlineWorkerConfiguration(
            farm_id=deadline_resources.farm.id,
            fleet=deadline_resources.fleet,
            region=region,
            allow_shutdown=True,
            worker_agent_install=PipInstall(
                requirement_specifiers=pip_install_requirement_specifiers,
                codeartifact=codeartifact,
            ),
            service_model_path=dst_path,
            file_mappings=file_mappings or None,
        )


@pytest.fixture(scope="session")
def ec2_worker_type(request: pytest.FixtureRequest) -> Generator[Type[DeadlineWorker], None, None]:
    # Allows overriding the base EC2InstanceWorker type with another derived type.
    operating_system = request.getfixturevalue("operating_system")

    if operating_system.name == "AL2023":
        yield PosixInstanceBuildWorker
    elif operating_system.name == "WIN2022":
        yield WindowsInstanceBuildWorker
    else:
        raise ValueError(
            'Invalid value provided for "operating_system", valid options are \'OperatingSystem("AL2023")\' or \'OperatingSystem("WIN2022")\'.'
        )


@pytest.fixture(scope="session")
def worker(
    request: pytest.FixtureRequest,
    worker_config: DeadlineWorkerConfiguration,
    ec2_worker_type: Type[EC2InstanceWorker],
) -> Generator[DeadlineWorker, None, None]:
    """
    Gets a DeadlineWorker for use in tests.

    Environment Variables:
        SUBNET_ID: The subnet ID to deploy the EC2 worker into.
            This is required for EC2 workers. Does not apply if USE_DOCKER_WORKER is true.
        SECURITY_GROUP_ID: The security group ID to deploy the EC2 worker into.
            This is required for EC2 workers. Does not apply if USE_DOCKER_WORKER is true.
        AMI_ID: The AMI ID to use for the Worker agent.
            Defaults to the latest AL2023 AMI.
            Does not apply if USE_DOCKER_WORKER is true.
        USE_DOCKER_WORKER: If set to "true", this fixture will create a Worker that runs in a local Docker container instead of an EC2 instance.
        KEEP_WORKER_AFTER_FAILURE: If set to "true", will not destroy the Worker when it fails. Useful for debugging. Default is "false"

    Returns:
        DeadlineWorker: Instance of the DeadlineWorker class that can be used to interact with the Worker.
    """

    worker: DeadlineWorker
    if os.environ.get("USE_DOCKER_WORKER", "").lower() == "true":
        LOG.info("Creating Docker worker")
        worker = DockerContainerWorker(
            configuration=worker_config,
        )
    else:
        LOG.info("Creating EC2 worker")
        ami_id = os.getenv("AMI_ID")
        subnet_id = os.getenv("SUBNET_ID")
        security_group_id = os.getenv("SECURITY_GROUP_ID")
        instance_type = os.getenv("WORKER_INSTANCE_TYPE", default="t3.micro")
        instance_shutdown_behavior = os.getenv("WORKER_INSTANCE_SHUTDOWN_BEHAVIOR", default="stop")

        assert subnet_id, "SUBNET_ID is required when deploying an EC2 worker"
        assert security_group_id, "SECURITY_GROUP_ID is required when deploying an EC2 worker"

        bootstrap_resources: BootstrapResources = request.getfixturevalue("bootstrap_resources")
        assert (
            bootstrap_resources.worker_instance_profile_name
        ), "Worker instance profile is required when deploying an EC2 worker"

        ec2_client = boto3.client("ec2")
        s3_client = boto3.client("s3")
        ssm_client = boto3.client("ssm")
        deadline_client = boto3.client("deadline")

        worker = ec2_worker_type(
            ec2_client=ec2_client,
            s3_client=s3_client,
            deadline_client=deadline_client,
            bootstrap_bucket_name=bootstrap_resources.bootstrap_bucket_name,
            ssm_client=ssm_client,
            override_ami_id=ami_id,
            subnet_id=subnet_id,
            security_group_id=security_group_id,
            instance_profile_name=bootstrap_resources.worker_instance_profile_name,
            configuration=worker_config,
            instance_type=instance_type,
            instance_shutdown_behavior=instance_shutdown_behavior,
        )

    def stop_worker():
        if request.session.testsfailed > 0:
            if os.getenv("KEEP_WORKER_AFTER_FAILURE", "false").lower() == "true":
                LOG.info("KEEP_WORKER_AFTER_FAILURE is set, not stopping worker")
                return

        try:
            worker.stop()
        except Exception as e:
            LOG.exception(f"Error while stopping worker: {e}")
            LOG.error(
                "Failed to stop worker. Resources may be left over that need to be cleaned up manually."
            )
            raise

    try:
        worker.start()
    except Exception as e:
        LOG.exception(f"Failed to start worker: {e}")
        LOG.info("Stopping worker because it failed to start")
        stop_worker()
        raise

    yield worker

    stop_worker()


@pytest.fixture(scope="session")
def deploy_job_attachment_resources() -> Generator[JobAttachmentManager, None, None]:
    """
    Deploys Job Attachments resources for integration tests

    Environment Variables:
        SERVICE_ACCOUNT_ID: The account ID the resources will be deployed to
        STAGE: The stage these resources are being deployed to
            Defaults to "dev"

    Returns:
        JobAttachmentManager: Class to manage Job Attachments resources
    """
    manager = JobAttachmentManager(
        s3_client=boto3.client("s3"),
        deadline_client=DeadlineClient(boto3.client("deadline")),
        account_id=os.environ["SERVICE_ACCOUNT_ID"],
        stage=os.getenv("STAGE", "dev"),
        bucket_name=os.environ["JOB_ATTACHMENTS_BUCKET"],
        farm_id=os.environ["FARM_ID"],
    )
    manager.deploy_resources()
    yield manager
    manager.cleanup_resources()


def _find_latest_service_model_file(service_name: str) -> str:
    loader = botocore.loaders.Loader(include_default_search_paths=True)
    full_name = os.path.join(
        service_name, loader.determine_latest_version(service_name, "service-2"), "service-2"
    )
    _, service_model_path = loader.load_data_with_path(full_name)
    service_model_files = glob.glob(f"{service_model_path}.*")
    if len(service_model_files) > 1:
        raise RuntimeError(
            f"Expected exactly one file to match glob '{service_model_path}.*, but got: {service_model_files}"
        )
    return service_model_files[0]


@pytest.fixture(scope="session")
def operating_system(request) -> OperatingSystem:
    if request.param == "linux":
        return OperatingSystem(name="AL2023")
    else:
        return OperatingSystem(name="WIN2022")
