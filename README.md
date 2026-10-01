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

## DCC UI Test Components

The standard package installation includes the dependencies needed to test a
graphical submitter:

```sh
pip install deadline-cloud-test-fixtures
```

The package provides four independent building blocks:

- `deadline_test_fixtures.deadline_mock`: an observable, scenario-driven
  Deadline REST-JSON server. `MockDeadlineServerProcess` runs it outside the
  pytest process so native accessibility waits cannot starve the server thread.
- `deadline_test_fixtures.xa11y`: cross-platform accessibility application
  discovery and reusable widget controls.
- `deadline_test_fixtures.job_bundle`: conventional case directories and
  structural bundle comparison with configurable normalization.
- `deadline_test_fixtures.images`: render comparison by dimensions and pixel
  tolerance.

Each DCC remains responsible for launching its host application and opening its
plugin. A typical offline test setup is:

```py
import os

from deadline_test_fixtures.deadline_mock import (
    MockDeadlineServerProcess,
    build_mock_environment,
    write_deadline_config,
)

with MockDeadlineServerProcess() as server:
    backend = server.backend
    assert backend is not None and server.base_url is not None
    write_deadline_config(
        tmp_path / "deadline.config",
        farm_id=backend.farm_id,
        queue_id=backend.queue_id,
        job_history_dir=tmp_path / "job_history",
    )
    env = build_mock_environment(
        os.environ,
        deadline_endpoint_url=server.base_url,
        config_path=tmp_path / "deadline.config",
        home_dir=tmp_path / "home",
    )
    # Launch the DCC with env, then locate its accessibility application.
```

`find_accessibility_app` locates a DCC host or submitter dialog by process ID
and an optional application-name prefix. Each DCC remains responsible for
launching, monitoring, and terminating its host process.

Deadline's service model injects a `management.` host prefix. A DCC subprocess
must disable that injection or redirect `management.*` to loopback; the default
`DEADLINE_CLOUD_MOCK_MODE=1` environment marker is provided for a test sidecar
to enable that behavior.

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
