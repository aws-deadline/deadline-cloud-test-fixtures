# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

import os
import posixpath
import sys
import tempfile
import uuid
from time import sleep
from typing import Any, Dict, Optional, List

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from botocore.loaders import Loader
from botocore.model import ServiceModel, OperationModel

from .constants import (
    JOB_ATTACHMENTS_ROOT_PREFIX,
    DEFAULT_CMF_CONFIG,
)


class DeadlineManager:
    """This class is responsible for setting up and tearing down the required components
    for the tests to be run."""

    deadline_service_model_bucket: Optional[str] = None
    deadline_endpoint: Optional[str] = None

    kms_client: BaseClient
    kms_key_metadata: Optional[Dict[str, Any]]

    deadline_client: DeadlineClient
    farm_id: Optional[str]
    queue_id: Optional[str]
    fleet_id: Optional[str]
    job_attachment_bucket: Optional[str]
    additional_queues: list[dict[str, Any]]
    deadline_model_dir: Optional[tempfile.TemporaryDirectory] = None

    MOCKED_SERVICE_VERSION = "2020-08-21"

    def __init__(self, should_add_deadline_models: bool = False) -> None:
        """
        Initializing the Deadline Manager
        """
        self.deadline_service_model_bucket = os.getenv("DEADLINE_SERVICE_MODEL_BUCKET")
        self.deadline_endpoint = os.getenv("DEADLINE_ENDPOINT")

        # Installing the deadline service models.
        if should_add_deadline_models:
            self.get_deadline_models()

        self.deadline_client = self._get_deadline_client(self.deadline_endpoint)

        # Create the KMS client
        self.kms_client = boto3.client("kms")

        self.farm_id: Optional[str] = None
        self.queue_id: Optional[str] = None
        self.fleet_id: Optional[str] = None
        self.additional_queues: list[dict[str, Any]] = []
        self.kms_key_metadata: Optional[dict[str, Any]] = None

    def get_deadline_models(self):
        """
        This function will download and install the models for deadline so we can use the deadline
        client.
        """
        if self.deadline_service_model_bucket is None:
            raise ValueError(
                "Environment variable DEADLINE_SERVICE_MODEL_BUCKET is not set. "
                "Unable to get deadline service model."
            )

        # Create the S3 client
        s3_client: BaseClient = boto3.client("s3")

        # Create a temp directory to store the model file
        self.deadline_model_dir = tempfile.TemporaryDirectory()
        service_model_dir = posixpath.join(
            self.deadline_model_dir.name, "deadline", self.MOCKED_SERVICE_VERSION
        )
        os.makedirs(service_model_dir)

        # Downloading the deadline models.
        s3_client.download_file(
            self.deadline_service_model_bucket,
            "service-2.json",
            posixpath.join(service_model_dir, "service-2.json"),
        )
        os.environ["AWS_DATA_PATH"] = self.deadline_model_dir.name

    def create_scaffolding(
        self,
        worker_role_arn: str,
        job_attachments_bucket: str,
        farm_name: str = uuid.uuid4().hex,
        queue_name: str = uuid.uuid4().hex,
        fleet_name: str = uuid.uuid4().hex,
    ) -> None:
        self.create_kms_key()
        self.create_farm(farm_name)
        self.create_queue(queue_name)
        self.add_job_attachments_bucket(job_attachments_bucket)
        self.create_fleet(fleet_name, worker_role_arn)
        self.queue_fleet_association()

    def create_kms_key(self) -> None:
        try:
            response: Dict[str, Any] = self.kms_client.create_key(
                Description="The KMS used for testing created by the "
                "DeadlineClientSoftwareTestScaffolding.",
                Tags=[{"TagKey": "Name", "TagValue": "DeadlineClientSoftwareTestScaffolding"}],
            )
        except ClientError as e:
            print("Failed to create CMK.", file=sys.stderr)
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            self.kms_key_metadata = response["KeyMetadata"]

            # We should always get a metadata when successful, this is for mypy.
            if self.kms_key_metadata:  # pragma: no cover
                print(f"Created CMK with id = {self.kms_key_metadata['KeyId']}")
                self.kms_client.enable_key(KeyId=self.kms_key_metadata["KeyId"])
                print(f"Enabled CMK with id = {self.kms_key_metadata['KeyId']}")

    def delete_kms_key(self) -> None:
        if (
            not hasattr(self, "kms_key_metadata")
            or self.kms_key_metadata is None
            or "KeyId" not in self.kms_key_metadata
        ):
            raise Exception("ERROR: Attempting to delete a KMS key when None was created!")

        try:
            # KMS keys by default are deleted in 30 days (this is their pending window).
            # 7 days is the fastest we can clean them up.
            pending_window = 7
            self.kms_client.schedule_key_deletion(
                KeyId=self.kms_key_metadata["KeyId"], PendingWindowInDays=pending_window
            )
        except ClientError as e:
            print(
                "Failed to schedule the deletion of CMK with id = "
                f"{self.kms_key_metadata['KeyId']}",
                file=sys.stderr,
            )
            print(f"The following error was raised: {e}", file=sys.stderr)
            raise
        else:
            print(f"Scheduled deletion of CMK with id = {self.kms_key_metadata['KeyId']}")
            self.kms_key_metadata = None

    def create_farm(self, farm_name: str) -> None:
        if (
            not hasattr(self, "kms_key_metadata")
            or self.kms_key_metadata is None
            or "Arn" not in self.kms_key_metadata
        ):
            raise Exception("ERROR: Attempting to create a farm without having creating a CMK.")

        try:
            response = self.deadline_client.create_farm(
                displayName=farm_name, kmsKeyArn=self.kms_key_metadata["Arn"]
            )
        except ClientError as e:
            print("Failed to create a farm.", file=sys.stderr)
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            self.farm_id = response["farmId"]
            print(f"Successfully create farm with id = {self.farm_id}")

    def delete_farm(self) -> None:
        if not hasattr(self, "farm_id") or not self.farm_id:
            raise Exception("ERROR: Attempting to delete a farm without having created one.")

        try:
            self.deadline_client.delete_farm(farmId=self.farm_id)
        except ClientError as e:
            print(f"Failed to delete farm with id = {self.farm_id}.", file=sys.stderr)
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            print(f"Successfully deleted farm with id = {self.farm_id}")
            self.farm_id = None

    # TODO: Add support for queue users with jobsRunAs
    def create_queue(self, queue_name: str) -> None:
        if not hasattr(self, "farm_id") or self.farm_id is None:
            raise Exception(
                "ERROR: Attempting to create a queue without having had created a farm!"
            )

        try:
            response = self.deadline_client.create_queue(
                displayName=queue_name,
                farmId=self.farm_id,
            )
        except ClientError as e:
            print(f"Failed to create queue with displayName = {queue_name}.", file=sys.stderr)
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            self.queue_id = response["queueId"]
            print(f"Successfully created queue with id = {self.queue_id}")

    def add_job_attachments_bucket(self, job_attachments_bucket: str):
        """Add a job attachments bucket to the queue"""
        self.deadline_client.update_queue(
            queueId=self.queue_id,
            farmId=self.farm_id,
            jobAttachmentSettings={
                "s3BucketName": job_attachments_bucket,
                "rootPrefix": JOB_ATTACHMENTS_ROOT_PREFIX,
            },
        )

    def create_additional_queue(self, **kwargs) -> Dict[str, Any]:
        """Create and add another queue to the deadline manager"""
        input = {"farmId": self.farm_id}
        input.update(kwargs)
        response = self.deadline_client.create_queue(**input)
        response = self.deadline_client.get_queue(
            farmId=input["farmId"], queueId=response["queueId"]
        )
        self.additional_queues.append(response)
        return response

    def delete_queue(self) -> None:
        if not hasattr(self, "farm_id") or not self.farm_id:
            raise Exception(
                "ERROR: Attempting to delete a queue without having had created a farm!"
            )

        if not hasattr(self, "queue_id") or not self.queue_id:
            raise Exception("ERROR: Attempting to delete a queue without having had created one!")

        try:
            self.deadline_client.delete_queue(queueId=self.queue_id, farmId=self.farm_id)
        except ClientError as e:
            print(f"Failed to delete queue with id = {self.queue_id}.", file=sys.stderr)
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            print(f"Successfully deleted queue with id = {self.queue_id}")
            self.queue_id = None

    def delete_additional_queues(self) -> None:
        """Delete all additional queues that have been added."""
        for queue in self.additional_queues:
            try:
                self.deadline_client.delete_queue(farmId=queue["farmId"], queueId=queue["queueId"])
            except Exception as e:
                print(f"delete queue exception {str(e)}")
                continue

    def create_fleet(self, fleet_name: str, worker_role_arn: str) -> None:
        if not hasattr(self, "farm_id") or not self.farm_id:
            raise Exception(
                "ERROR: Attempting to create a fleet without having had created a farm!"
            )
        try:
            response = self.deadline_client.create_fleet(
                farmId=self.farm_id,
                displayName=fleet_name,
                roleArn=worker_role_arn,
                configuration=DEFAULT_CMF_CONFIG,
            )
        except ClientError as e:
            print(f"Failed to create fleet with displayName = {fleet_name}.", file=sys.stderr)
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            self.fleet_id = response["fleetId"]
            self.wait_for_desired_fleet_status(
                desired_status="ACTIVE", allowed_status=["ACTIVE", "CREATE_IN_PROGRESS"]
            )
            print(f"Successfully created a fleet with id = {self.fleet_id}")

    # Temporary until we have waiters
    def wait_for_desired_fleet_status(self, desired_status: str, allowed_status: List[str]) -> None:
        max_retries = 10
        fleet_status = None
        retry_count = 0
        while fleet_status != desired_status and retry_count < max_retries:
            response = self.deadline_client.get_fleet(fleetId=self.fleet_id, farmId=self.farm_id)

            fleet_status = response["status"]

            if fleet_status not in allowed_status:
                raise ValueError(
                    f"fleet entered a nonvalid status ({fleet_status}) while "
                    f"waiting for the desired status: {desired_status}."
                )

            if fleet_status == desired_status:
                return response

            print(f"Fleet status: {fleet_status}\nChecking again...")
            retry_count += 1
            sleep(10)

        raise ValueError(
            f"Timed out waiting for fleet status to reach the desired status {desired_status}."
        )

    def queue_fleet_association(self) -> None:
        if not hasattr(self, "farm_id") or not self.farm_id:
            raise Exception("ERROR: Attempting to queue a fleet without having had created a farm!")

        if not hasattr(self, "queue_id") or not self.queue_id:
            raise Exception("ERROR: Attempting to queue a fleet without creating a queue")

        if not hasattr(self, "fleet_id") or not self.fleet_id:
            raise Exception("ERROR: Attempting to queue a fleet without having had created one!")

        try:
            self.deadline_client.create_queue_fleet_association(
                farmId=self.farm_id, queueId=self.queue_id, fleetId=self.fleet_id
            )
        except ClientError as e:
            print(f"Failed to associate fleet with id = {self.fleet_id}.", file=sys.stderr)
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            print(f"Successfully queued fleet with id = {self.fleet_id}")

    # Temporary until we have waiters
    def stop_queue_fleet_associations_and_wait(self) -> None:
        self.deadline_client.update_queue_fleet_association(
            farmId=self.farm_id,
            queueId=self.queue_id,
            fleetId=self.fleet_id,
            status="CANCEL_WORK",
        )
        max_retries = 10
        retry_count = 0
        qfa_status = None
        allowed_status = ["STOPPED", "CANCEL_WORK"]
        while qfa_status != "STOPPED" and retry_count < max_retries:
            response = self.deadline_client.get_queue_fleet_association(
                farmId=self.farm_id, queueId=self.queue_id, fleetId=self.fleet_id
            )

            qfa_status = response["status"]

            if qfa_status not in allowed_status:
                raise ValueError(
                    f"Association entered a nonvalid status ({qfa_status}) while "
                    f"waiting for the desired status: STOPPED"
                )

            if qfa_status == "STOPPED":
                return response

            print(f"Queue Fleet Association: {qfa_status}\nChecking again...")
            retry_count += 1
            sleep(10)
        raise ValueError("Timed out waiting for association to reach a STOPPED status.")

    def delete_fleet(self) -> None:
        if not hasattr(self, "farm_id") or not self.farm_id:
            raise Exception(
                "ERROR: Attempting to delete a fleet without having had created a farm!"
            )

        if not hasattr(self, "fleet_id") or not self.fleet_id:
            raise Exception("ERROR: Attempting to delete a fleet when none was created!")

        try:
            # Delete queue fleet association.
            self.stop_queue_fleet_associations_and_wait()
            self.deadline_client.delete_queue_fleet_association(
                farmId=self.farm_id, queueId=self.queue_id, fleetId=self.fleet_id
            )
            # Deleting the fleet.
            self.deadline_client.delete_fleet(farmId=self.farm_id, fleetId=self.fleet_id)
        except ClientError as e:
            print(
                f"ERROR: Failed to delete delete fleet with id = {self.fleet_id}", file=sys.stderr
            )
            print(f"The following exception was raised: {e}", file=sys.stderr)
            raise
        else:
            print(f"Successfully deleted fleet with id = {self.fleet_id}")
            self.fleet_id = None

    def cleanup_scaffolding(self) -> None:
        # Only deleting the fleet if we have a fleet.
        if hasattr(self, "fleet_id") and self.fleet_id:
            self.delete_fleet()

        if hasattr(self, "farm_id") and self.farm_id:
            # Only deleting the queue if we have a queue.
            if hasattr(self, "queue_id") and self.queue_id:
                self.delete_queue()

        self.delete_farm()

        # Only deleting the kms key if we have a kms key.
        if hasattr(self, "kms_key_metadata") and self.kms_key_metadata:
            self.delete_kms_key()

    def _get_deadline_client(self, deadline_endpoint: Optional[str]) -> DeadlineClient:
        """Create a DeadlineClient shim layer over an actual boto client"""
        self.session = boto3.Session()
        real_deadline_client = self.session.client(
            "deadline",
            endpoint_url=deadline_endpoint,
        )

        return DeadlineClient(real_deadline_client)


class DeadlineClient:
    """
    A shim layer for boto Deadline client. This class will check if a method exists on the real
    boto3 Deadline client and call it if it exists. If it doesn't exist, an AttributeError will be raised.
    """

    _real_client: Any

    def __init__(self, real_client: Any) -> None:
        self._real_client = real_client

    def create_farm(self, *args, **kwargs) -> Any:
        create_farm_input_members = self._get_deadline_api_input_shape("CreateFarm")
        if "displayName" not in create_farm_input_members and "name" in create_farm_input_members:
            kwargs["name"] = kwargs.pop("displayName")
        return self._real_client.create_farm(*args, **kwargs)

    def create_fleet(self, *args, **kwargs) -> Any:
        create_fleet_input_members = self._get_deadline_api_input_shape("CreateFleet")
        if "displayName" not in create_fleet_input_members and "name" in create_fleet_input_members:
            kwargs["name"] = kwargs.pop("displayName")
        if (
            "roleArn" not in create_fleet_input_members
            and "workeRoleArn" in create_fleet_input_members
        ):
            kwargs["workerRoleArn"] = kwargs.pop("roleArn")
        return self._real_client.create_fleet(*args, **kwargs)

    def get_fleet(self, *args, **kwargs) -> Any:
        response = self._real_client.get_fleet(*args, **kwargs)
        if "name" in response and "displayName" not in response:
            response["displayName"] = response["name"]
            del response["name"]
        if "state" in response and "status" not in response:
            response["status"] = response["state"]
            del response["state"]
        if "type" in response:
            del response["type"]
        return response

    def get_queue_fleet_association(self, *args, **kwargs) -> Any:
        response = self._real_client.get_queue_fleet_association(*args, **kwargs)
        if "state" in response and "status" not in response:
            response["status"] = response["state"]
            del response["state"]
        return response

    def create_queue(self, *args, **kwargs) -> Any:
        create_queue_input_members = self._get_deadline_api_input_shape("CreateQueue")
        if "displayName" not in create_queue_input_members and "name" in create_queue_input_members:
            kwargs["name"] = kwargs.pop("displayName")
        return self._real_client.create_queue(*args, **kwargs)

    def create_queue_fleet_association(self, *args, **kwargs) -> Any:
        create_queue_fleet_association_method_name: Optional[str]
        create_queue_fleet_association_method: Optional[str]

        for create_queue_fleet_association_method_name in (
            "put_queue_fleet_association",
            "create_queue_fleet_association",
        ):
            create_queue_fleet_association_method = getattr(
                self._real_client, create_queue_fleet_association_method_name, None
            )
            if create_queue_fleet_association_method:
                break
            else:
                create_queue_fleet_association_method = None

        # mypy complains about they kwargs type
        return create_queue_fleet_association_method(*args, **kwargs)  # type: ignore

    def create_job(self, *args, **kwargs) -> Any:
        create_job_input_members = self._get_deadline_api_input_shape("CreateJob")
        # revert to old parameter names if old service model is used
        if "maxRetriesPerTask" in kwargs:
            if "maxErrorsPerTask" in create_job_input_members:
                kwargs["maxErrorsPerTask"] = kwargs.pop("maxRetriesPerTask")
        if "template" in kwargs:
            if "jobTemplate" in create_job_input_members:
                kwargs["jobTemplate"] = kwargs.pop("template")
                kwargs["jobTemplateType"] = kwargs.pop("templateType")
                if "parameters" in kwargs:
                    kwargs["jobParameters"] = kwargs.pop("parameters")
        if "targetTaskRunStatus" in kwargs:
            if "initialState" in create_job_input_members:
                kwargs["initialState"] = kwargs.pop("targetTaskRunStatus")
        if "priority" not in kwargs:
            kwargs["priority"] = 50
        return self._real_client.create_job(*args, **kwargs)

    def update_queue_fleet_association(self, *args, **kwargs) -> Any:
        update_queue_fleet_association_method_name: Optional[str]
        update_queue_fleet_association_method: Optional[str]

        for update_queue_fleet_association_method_name in (
            "update_queue_fleet_association",
            "update_queue_fleet_association_state",
        ):
            update_queue_fleet_association_method = getattr(
                self._real_client, update_queue_fleet_association_method_name, None
            )
            if update_queue_fleet_association_method:
                break
            else:
                update_queue_fleet_association_method = None

        if update_queue_fleet_association_method_name == "update_queue_fleet_association":
            # mypy complains about they kwargs type
            return update_queue_fleet_association_method(*args, **kwargs)  # type: ignore

        if update_queue_fleet_association_method_name == "update_queue_fleet_association_state":
            kwargs["state"] = kwargs.pop("status")
            # mypy complains about they kwargs type
            return update_queue_fleet_association_method(*args, **kwargs)  # type: ignore

    def _get_deadline_api_input_shape(self, api_name: str) -> dict[str, Any]:
        """
        Given a string name of an API e.g. CreateJob, returns the shape of the
        inputs to that API.
        """
        api_model = self._get_deadline_api_model(api_name)
        if api_model:
            return api_model.input_shape.members
        return {}

    def _get_deadline_api_model(self, api_name: str) -> Optional[OperationModel]:
        """
        Given a string name of an API e.g. CreateJob, returns the OperationModel
        for that API from the service model.
        """
        data_model_path = os.getenv("AWS_DATA_PATH")
        loader = Loader(extra_search_paths=[data_model_path] if data_model_path is not None else [])
        deadline_service_description = loader.load_service_model("deadline", "service-2")
        deadline_service_model = ServiceModel(deadline_service_description, service_name="deadline")
        return OperationModel(
            deadline_service_description["operations"][api_name], deadline_service_model
        )

    def __getattr__(self, __name: str) -> Any:
        """
        Respond to unknown method calls by calling the underlying _real_client
        If the underlying _real_client does not have a given method, an AttributeError
        will be raised.
        Note that __getattr__ is only called if the attribute cannot otherwise be found,
        so if this class alread has the called method defined, __getattr__ will not be called.
        This is in opposition to __getattribute__ which is called by default.
        """

        def method(*args, **kwargs):
            return getattr(self._real_client, __name)(*args, **kwargs)

        return method
