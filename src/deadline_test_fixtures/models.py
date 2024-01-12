# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import json
import os
import re
import tempfile
from abc import ABC, abstractproperty
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Generator, Literal


@dataclass(frozen=True)
class JobAttachmentSettings:
    bucket_name: str
    root_prefix: str

    def as_queue_settings(self) -> dict:
        return {
            "s3BucketName": self.bucket_name,
            "rootPrefix": self.root_prefix,
        }


@dataclass(frozen=True)
class PosixSessionUser:
    user: str
    group: str


@dataclass(frozen=True)
class JobRunAsUser:
    posix: PosixSessionUser
    runAs: Literal["QUEUE_CONFIGURED_USER", "WORKER_AGENT_USER"]


@dataclass(frozen=True)
class CodeArtifactRepositoryInfo:
    region: str
    domain: str
    domain_owner: str
    repository: str

    @property
    def domain_arn(self) -> str:
        return f"arn:aws:codeartifact:{self.region}:{self.domain_owner}:domain/{self.domain}"

    @property
    def repository_arn(self) -> str:
        return f"arn:aws:codeartifact:{self.region}:{self.domain_owner}:repository/{self.domain}/{self.repository}"


@dataclass(frozen=True)
class S3Object:
    bucket: str
    key: str

    @staticmethod
    def from_uri(uri: str) -> S3Object:
        match = re.match(r"s3://(.+?)/(.+)", uri)
        assert isinstance(match, re.Match), f"Cannot retrieve S3 bucket and key from URI: {uri}"
        bucket, key = match.groups()
        return S3Object(
            bucket=bucket,
            key=key,
        )

    @property
    def arn(self) -> str:
        return f"arn:aws:s3:::{self.bucket}/{self.key}"

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


class PathMappable(ABC):
    """Interface for classes that can be path mapped"""

    @abstractproperty
    def path_mappings(self) -> list[tuple[str, str]]:
        pass


@dataclass(frozen=True)
class ServiceModel:
    file_path: str
    api_version: str
    service_name: str

    @staticmethod
    def from_json_file(path: str) -> ServiceModel:
        with open(path) as f:
            model = json.load(f)
        return ServiceModel(
            file_path=path,
            api_version=model["metadata"]["apiVersion"],
            service_name=model["metadata"]["serviceId"],
        )

    @contextmanager
    def install(self) -> Generator[str, None, None]:
        """
        Copies the model to a temporary directory in the structure expected by boto
        and sets the AWS_DATA_PATH environment variable to it
        """
        try:
            old_aws_data_path = os.environ.get("AWS_DATA_PATH")
            src_file = Path(self.file_path)
            with tempfile.TemporaryDirectory() as tmpdir:
                json_path = Path(tmpdir) / self.service_name / self.api_version / "service-2.json"
                json_path.parent.mkdir(parents=True)
                json_path.write_text(src_file.read_text())
                os.environ["AWS_DATA_PATH"] = tmpdir
                yield str(tmpdir)
        finally:
            if old_aws_data_path:
                os.environ["AWS_DATA_PATH"] = old_aws_data_path
            else:
                del os.environ["AWS_DATA_PATH"]

    @property
    def install_command(self) -> str:
        return " ".join(
            [
                "aws",
                "configure",
                "add-model",
                "--service-model",
                f"file://{self.file_path}",
                *(["--service-name", self.service_name] if self.service_name else []),
            ]
        )


@dataclass(frozen=True)
class PipInstall:  # pragma: no cover
    requirement_specifiers: list[str]
    """See https://peps.python.org/pep-0508/"""
    upgrade_pip: bool = True
    find_links: list[str] | None = None
    no_deps: bool = False
    force_reinstall: bool = False
    codeartifact: CodeArtifactRepositoryInfo | None = None

    def __post_init__(self) -> None:
        assert len(
            self.requirement_specifiers
        ), "At least one requirement specifier is required, but got 0"

    @property
    def install_args(self) -> list[str]:
        args = []
        if self.find_links:
            args.append(f"--find-links={','.join(self.find_links)}")
        if self.no_deps:
            args.append("--no-deps")
        if self.force_reinstall:
            args.append("--force-reinstall")
        return args

    @property
    def install_command(self) -> str:
        cmds = []

        if self.codeartifact:
            cmds.append(
                "aws codeartifact login --tool pip "
                + f"--domain {self.codeartifact.domain} "
                + f"--domain-owner {self.codeartifact.domain_owner} "
                + f"--repository {self.codeartifact.repository} "
            )

        if self.upgrade_pip:
            cmds.append("pip install --upgrade pip")

        cmds.append(
            " ".join(
                [
                    "pip",
                    "install",
                    *self.install_args,
                    *self.requirement_specifiers,
                ]
            )
        )

        return " && ".join(cmds)
