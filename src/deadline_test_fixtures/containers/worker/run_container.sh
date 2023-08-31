#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

set -eu

tmp_env_file=$(mktemp -p $(pwd))
for var in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_DEFAULT_REGION FARM_ID FLEET_ID
do
    if test "${!var:-}" == "";
    then
        echo "ERROR: Environment variable $var must be set"
        exit 1
    fi
    echo -n "$var=" >> $tmp_env_file
    printenv $var >> $tmp_env_file
done

if test "$PIP_INDEX_URL" != ""; then
    echo "PIP_INDEX_URL=$PIP_INDEX_URL" >> $tmp_env_file
fi

FILE_MAPPINGS=${FILE_MAPPINGS:-}
if [ -z "$FILE_MAPPINGS" ]; then
    # Put a dummy file so Dockerfile COPY command still has something to copy
    dummy_file_path="$(pwd)/file_mappings/dummy_file"
    touch "$dummy_file_path"
    FILE_MAPPINGS="{\"$dummy_file_path\": \"$dummy_file_path\"}"
fi

# Use Docker BuildKit to use new features like heredoc support in Dockerfile
container_image_tag=agent_integ
DOCKER_BUILDKIT=1 docker build . -q -t $container_image_tag \
    --build-arg AWS_ACCESS_KEY_ID \
    --build-arg AWS_SECRET_ACCESS_KEY \
    --build-arg AWS_SESSION_TOKEN \
    --build-arg AWS_DEFAULT_REGION \
    --build-arg AGENT_USER \
    --build-arg JOB_USER \
    --build-arg SHARED_GROUP \
    --build-arg CONFIGURE_WORKER_AGENT_CMD \
    --build-arg FILE_MAPPINGS

docker run \
    --rm \
    --detach \
    --name integ_worker_agent \
    --env-file $tmp_env_file \
    -h worker-integ.environment.internal \
    --cidfile $(pwd)/.container_id \
    $container_image_tag:latest

rm -f $tmp_env_file