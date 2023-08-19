from __future__ import annotations

import re
from dataclasses import dataclass


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


@dataclass(frozen=True)
class ServiceModel:
    install_command: str

    @staticmethod
    def from_s3(
        *,
        object: S3Object,
        local_filename: str,
        service_name: str | None = None,
    ) -> ServiceModel:
        cmd = " && ".join(
            [
                f"aws s3 cp {object.uri} {local_filename}",
                # fmt: off
            " ".join(
                [
                    "aws",
                    "configure",
                    "add-model",
                    "--service-model",
                    f"file://{local_filename}",
                    *(["--service-name", service_name] if service_name else []),
                ]
            ),
                # fmt: on
            ]
        )
        return ServiceModel(install_command=cmd)

    @staticmethod
    def from_local_file(*, local_file_path: str, service_name: str | None = None) -> ServiceModel:
        cmd = " ".join(
            [
                "aws",
                "configure",
                "add-model",
                "--service-model",
                f"file://{local_file_path}",
                *(["--service-name", service_name] if service_name else []),
            ]
        )
        return ServiceModel(cmd)
