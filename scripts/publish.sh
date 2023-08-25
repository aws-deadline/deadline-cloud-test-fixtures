#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

set -euo pipefail

hatch clean
hatch run lint
hatch run test:test
hatch build

export TWINE_USERNAME=aws
export TWINE_PASSWORD=`aws codeartifact get-authorization-token --domain $CODEARTIFACT_DOMAIN --domain-owner $CODEARTIFACT_ACCOUNT_ID --query authorizationToken --output text`
export TWINE_REPOSITORY_URL=`aws codeartifact get-repository-endpoint --domain $CODEARTIFACT_DOMAIN --domain-owner $CODEARTIFACT_ACCOUNT_ID --repository $CODEARTIFACT_REPOSITORY --format pypi --query repositoryEndpoint --output text`
twine upload dist/*
