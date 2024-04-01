# AWS Deadline Cloud Test Fixtures

[![pypi](https://img.shields.io/pypi/v/deadline-cloud-test-fixtures.svg?style=flat)](https://pypi.python.org/pypi/deadline-cloud-test-fixtures)
[![python](https://img.shields.io/pypi/pyversions/deadline-cloud-test-fixtures.svg?style=flat)](https://pypi.python.org/pypi/deadline-cloud-test-fixtures)
[![license](https://img.shields.io/pypi/l/deadline-cloud-test-fixtures.svg?style=flat)](https://github.com/aws-deadline/deadline-cloud-test-fixtures/blob/mainline/LICENSE)

This package contains pytest fixtures that are used to test AWS Deadline Cloud Python packages.

## Usage

To use this package:
1. Install it into your test environment
1. Configure environment variables needed for your tests (see [src/deadline_test_fixtures/example_config.sh](https://github.com/casillas2/deadline-cloud-test-fixtures/blob/mainline/src/deadline_test_fixtures/example_config.sh) for available options)
1. Use the fixtures in your tests (see [src/deadline_test_fixtures/fixtures.py](https://github.com/casillas2/deadline-cloud-test-fixtures/blob/mainline/src/deadline_test_fixtures/fixtures.py) for available fixtures)

For example, to use the `worker` fixture:

```py
from deadline_test_fixtures import DeadlineWorker

def test_something_with_the_worker(worker: DeadlineWorker) -> None:
    # GIVEN
    worker.start()

    # WHEN
    result = worker.send_command("some command")

    # THEN
    assert result.stdout == "expected output"
```

You can also import the classes from this package directly to build your own fixtures

```py
# double_worker.py
from deadline_test_fixtures import (
    DeadlineWorker,
    EC2InstanceWorker,
    DockerContainerWorker,
)

class DoubleWorker(DeadlineWorker):

    def __init__(
        self,
        # args...
    ) -> None:
        self.ec2_worker = EC2InstanceWorker(
            # args...
        )
        self.docker_worker = DockerContainerWorker(
            # args...
        )
    
    def start(self) -> None:
        self.ec2_worker.start()
        self.docker_worker.start()
    
    # etc.


# test_something.py
from .double_worker import DoubleWorker

import pytest

@pytest.fixture
def double_worker() -> DoubleWorker:
    return DoubleWorker(
        # args...
    )

def test_something(double_worker: DoubleWorker) -> None:
    # GIVEN
    double_worker.start()

    # etc.
```

## Telemetry

This library collects telemetry data by default. Telemetry events contain non-personally-identifiable information that helps us understand how users interact with our software so we know what features our customers use, and/or what existing pain points are.

You can opt out of telemetry data collection by either:

1. Setting the environment variable: `DEADLINE_CLOUD_TELEMETRY_OPT_OUT=true`
2. Setting the config file: `deadline config set telemetry.opt_out true`

Note that setting the environment variable supersedes the config file setting.

## Build / Test / Release

### Build the package.
```
hatch build
```

### Run tests
```
hatch run test
```

### Run linting
```
hatch run lint
```

### Run formating
```
hatch run fmt
```

### Run a tests for all supported Python versions.
```
hatch run all:test
```

## Compatibility

This library requires:

1. Python 3.9 or higher; and
2. Linux, MacOS, or Windows operating system.

## Versioning

This package's version follows [Semantic Versioning 2.0](https://semver.org/), but is still considered to be in its 
initial development, thus backwards incompatible versions are denoted by minor version bumps. To help illustrate how
versions will increment during this initial development stage, they are described below:

1. The MAJOR version is currently 0, indicating initial development. 
2. The MINOR version is currently incremented when backwards incompatible changes are introduced to the public API. 
3. The PATCH version is currently incremented when bug fixes or backwards compatible changes are introduced to the public API. 

## Downloading

You can download this package from:
- [GitHub releases](https://github.com/casillas2/deadline-cloud-test-fixtures/releases)

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

