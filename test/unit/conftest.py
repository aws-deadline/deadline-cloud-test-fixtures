# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from typing import Generator
from unittest.mock import patch

import pytest


@pytest.fixture(scope="function", autouse=True)
def boto_config() -> Generator[dict[str, str], None, None]:
    config = {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SECURITY_TOKEN": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }
    with patch.dict("os.environ", config):
        yield config
