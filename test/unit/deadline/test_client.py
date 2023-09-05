# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
import pytest
from unittest.mock import MagicMock, patch
from deadline_test_fixtures import DeadlineClient

MOCK_FARM_NAME = "test-farm"
MOCK_FLEET_NAME = "test-fleet"
MOCK_QUEUE_NAME = "test-queue"


class TestDeadlineClient:
    def test_deadline_client_pass_through(self) -> None:
        """
        Confirm that DeadlineClient passes through unknown methods to the underlying client
        but just executes known methods.
        """

        class FakeClient:
            def fake_deadline_client_has_this(self) -> str:
                return "from fake client"

            def but_not_this(self) -> str:
                return "from fake client"

        class FakeDeadlineClient(DeadlineClient):
            def fake_deadline_client_has_this(self) -> str:
                return "from fake deadline client"

        fake_client = FakeClient()
        deadline_client = FakeDeadlineClient(fake_client)

        assert deadline_client.fake_deadline_client_has_this() == "from fake deadline client"
        assert deadline_client.but_not_this() == "from fake client"

    @pytest.mark.parametrize(
        "kwargs_input, name_in_model, kwargs_output",
        [
            pytest.param(
                {"displayName": MOCK_FARM_NAME},
                "name",
                {"name": MOCK_FARM_NAME},
                id="DisplayNameInSubmissionNotModel",
            ),
            pytest.param(
                {"displayName": MOCK_FARM_NAME},
                "displayName",
                {"displayName": MOCK_FARM_NAME},
                id="DisplayNameInSubmissionAndModel",
            ),
        ],
    )
    def test_create_farm_name_to_display_name(
        self, kwargs_input, name_in_model, kwargs_output
    ) -> None:
        """
        create_farm will be updated so that name is renamed to displayName. Here we
        make sure that the shim is doing its job of:
        1. Calling the underlying client method
        2. Replacing the appropriate key if needed
        """
        fake_client = MagicMock()
        deadline_client = DeadlineClient(fake_client)

        with patch.object(deadline_client, "_get_deadline_api_input_shape") as input_shape_mock:
            input_shape_mock.return_value = {name_in_model: MOCK_FARM_NAME}
            deadline_client.create_farm(**kwargs_input)
        fake_client.create_farm.assert_called_once_with(**kwargs_output)

    @pytest.mark.parametrize(
        "kwargs_input, name_in_model, kwargs_output",
        [
            pytest.param(
                {"displayName": MOCK_FLEET_NAME},
                "name",
                {"name": MOCK_FLEET_NAME},
                id="DisplayNameInSubmissionNotModel",
            ),
            pytest.param(
                {"displayName": MOCK_FLEET_NAME},
                "displayName",
                {"displayName": MOCK_FLEET_NAME},
                id="DisplayNameInSubmissionAndModel",
            ),
        ],
    )
    def test_create_fleet_name_to_display_name(
        self, kwargs_input, name_in_model, kwargs_output
    ) -> None:
        """
        create_fleet will be updated so that name is renamed to displayName.
        Here we make sure that the shim is doing its job of:
        1. Calling the underlying client method
        2. Replacing the appropriate key if needed
        """
        fake_client = MagicMock()
        deadline_client = DeadlineClient(fake_client)

        with patch.object(deadline_client, "_get_deadline_api_input_shape") as input_shape_mock:
            input_shape_mock.return_value = {name_in_model: MOCK_FLEET_NAME}
            deadline_client.create_fleet(**kwargs_input)
        fake_client.create_fleet.assert_called_once_with(**kwargs_output)

    @pytest.mark.parametrize(
        "kwargs_input, name_in_model, kwargs_output",
        [
            pytest.param(
                {"displayName": MOCK_QUEUE_NAME},
                "name",
                {"name": MOCK_QUEUE_NAME},
                id="DisplayNameInSubmissionNotModel",
            ),
            pytest.param(
                {"displayName": MOCK_QUEUE_NAME},
                "displayName",
                {"displayName": MOCK_QUEUE_NAME},
                id="DisplayNameInSubmissionAndModel",
            ),
        ],
    )
    def test_create_queue_name_to_display_name(
        self, kwargs_input, name_in_model, kwargs_output
    ) -> None:
        """
        create_queue will be updated so that name is renamed to displayName.
        Here we make sure that the shim is doing its job of:
        1. Calling the underlying client method
        2. Replacing the appropriate key if needed
        """
        fake_client = MagicMock()
        deadline_client = DeadlineClient(fake_client)

        with patch.object(deadline_client, "_get_deadline_api_input_shape") as input_shape_mock:
            input_shape_mock.return_value = {name_in_model: MOCK_QUEUE_NAME}
            deadline_client.create_queue(**kwargs_input)
        fake_client.create_queue.assert_called_once_with(**kwargs_output)

    @pytest.mark.parametrize(
        "kwargs_input, kwargs_output, names_in_model",
        [
            pytest.param(
                {"template": "", "templateType": "", "parameters": ""},
                {"template": "", "templateType": "", "parameters": ""},
                ["template", "templateType", "parameters"],
                id="jobTemplate_NewAPI",
            ),
            pytest.param(
                {"template": "", "templateType": "", "parameters": ""},
                {
                    "jobTemplate": "",
                    "jobTemplateType": "",
                    "jobParameters": "",
                },
                [
                    "jobTemplate",
                    "jobTemplateType",
                    "jobParameters",
                ],
                id="jobTemplate_OldAPI",
            ),
            pytest.param(
                {"template": "", "templateType": "", "parameters": "", "initialState": ""},
                {"jobTemplate": "", "jobTemplateType": "", "jobParameters": "", "initialState": ""},
                ["jobTemplate", "jobTemplateType", "jobParameters", "initialState"],
                id="jobTemplate_StateToState",
            ),
            pytest.param(
                {"template": "", "templateType": "", "parameters": "", "targetTaskRunStatus": ""},
                {
                    "jobTemplate": "",
                    "jobTemplateType": "",
                    "jobParameters": "",
                    "initialState": "",
                },
                ["jobTemplate", "jobTemplateType", "jobParameters", "initialState"],
                id="jobTemplate_StatusToState",
            ),
            pytest.param(
                {"template": "", "templateType": "", "parameters": "", "targetTaskRunStatus": ""},
                {
                    "jobTemplate": "",
                    "jobTemplateType": "",
                    "jobParameters": "",
                    "targetTaskRunStatus": "",
                },
                ["jobTemplate", "jobTemplateType", "jobParameters", "targetTaskRunStatus"],
                id="jobTemplate_StatusToStatus",
            ),
        ],
    )
    def test_create_job_old_api_compatibility(
        self, kwargs_input, kwargs_output, names_in_model
    ) -> None:
        """
        create_job will be updated so that template is renamed to
        jobTemplate. Here we make sure that the shim is doing its job of:
        1. Calling the underlying client method
        2. Replacing the appropriate key

        """
        fake_client = MagicMock()
        kwargs_output["priority"] = 50
        model: dict = {k: "" for k in names_in_model}
        model["priority"] = 50
        deadline_client = DeadlineClient(fake_client)
        with patch.object(deadline_client, "_get_deadline_api_input_shape") as input_shape_mock:
            input_shape_mock.return_value = kwargs_output
            deadline_client.create_job(**kwargs_input)
        fake_client.create_job.assert_called_once_with(**kwargs_output)

    def test_get_fleet_name_state_to_displayname_status_remove_type(self) -> None:
        """
        get_fleet will be updated such that "name" will be replaced with "displayName"
        and "state" will be replaced with "status". "type" will be removed.
        Here we make sure that the shim is doing it's job of:
        1. Calling the underlying client method
        2. Replacing the appropriate keys
        """
        fake_client = MagicMock()
        fake_client.get_fleet.return_value = {
            "name": "fleet1",
            "state": "ACTIVE",
            "type": "CUSTOMER_MANAGER",
        }
        deadline_client = DeadlineClient(fake_client)
        response = deadline_client.get_fleet("fleetid-somefleet")

        assert "name" not in response
        assert "displayName" in response
        assert "state" not in response
        assert "status" in response
        assert "type" not in response
        assert response["displayName"] == "fleet1"
        assert response["status"] == "ACTIVE"
        fake_client.get_fleet.assert_called_once_with("fleetid-somefleet")

    def test_get_queue_fleet_association_state_to_status(self) -> None:
        """
        get_queue_fleet_association will be updated such that  "state" will be replaced with "status".
        Here we make sure that the shim is doing it's job of:
        1. Calling the underlying client method
        2. Replacing the appropriate keys
        """
        fake_client = MagicMock()
        fake_client.get_queue_fleet_association.return_value = {
            "state": "STOPPED",
        }
        deadline_client = DeadlineClient(fake_client)
        response = deadline_client.get_queue_fleet_association(
            "fake-farm-id", "fake-queue-id", "fake-fleet-id"
        )

        assert "state" not in response
        assert "status" in response
        assert response["status"] == "STOPPED"
        fake_client.get_queue_fleet_association.assert_called_once_with(
            "fake-farm-id", "fake-queue-id", "fake-fleet-id"
        )
