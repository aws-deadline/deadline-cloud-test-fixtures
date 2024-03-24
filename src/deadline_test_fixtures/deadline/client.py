# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import logging
import os
from typing import Any, Optional

from botocore.loaders import Loader
from botocore.model import ServiceModel, OperationModel

LOG = logging.getLogger(__name__)


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

        if "configuration" in create_fleet_input_members:
            customer_managed_members = (
                create_fleet_input_members["configuration"].members["customerManaged"].members
            )

            if (
                "workerCapabilities" not in customer_managed_members
                and "workerRequirements" in customer_managed_members
            ):
                if (
                    "customerManaged" in kwargs["configuration"]
                    and "workerCapabilities" in kwargs["configuration"]["customerManaged"]
                ):
                    kwargs["configuration"]["customerManaged"]["workerRequirements"] = kwargs[
                        "configuration"
                    ]["customerManaged"].pop("workerCapabilities")

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
