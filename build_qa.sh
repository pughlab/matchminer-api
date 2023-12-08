#!/bin/bash
# tells the shell to mark variables for export,
# which means they will be available to child processes.
set -a
#sources the environment file,
# effectively loading the environment variables into the current shell.
. /etc/environment
# unsets the export flag.
set +a
SELFDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
uuid=$(uuidgen)

echo $SELFDIR

TECHNA_HOSTNAME=qa02.technainstitute.net
TECHNA_ROLE=qa
TECHNA_REGISTRY_PORT=443
TECHNA_REGISTRY_ENDPOINT=docker-registry.uhn.ca

MM_API_DB_CONTAINER_IMAGE_NAME=matchminer-api-qa
MM_API_DB_CONTAINER_IMAGE_LOCATION=$TECHNA_REGISTRY_ENDPOINT:$TECHNA_REGISTRY_PORT/$MM_API_DB_CONTAINER_IMAGE_NAME

docker build -f Dockerfile-dev -t ${MM_API_DB_CONTAINER_IMAGE_LOCATION}:$uuid $SELFDIR