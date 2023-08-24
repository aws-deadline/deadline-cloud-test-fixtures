# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Generator
from unittest import mock
from unittest.mock import patch

import pytest

from deadline_test_scaffolding import DeadlineManager


@pytest.fixture()
def mock_get_deadline_models():
    with mock.patch.object(DeadlineManager, "get_deadline_models") as mocked_get_deadline_models:
        yield mocked_get_deadline_models


@pytest.fixture(scope="function")
def boto_config() -> Generator[None, None, None]:
    updated_environment = {
        "AWS_ACCESS_KEY_ID": "ACCESSKEY",
        "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "AWS_DEFAULT_REGION": "us-west-2",
    }
    with patch.dict("os.environ", updated_environment):
        yield
