#!/bin/bash
#docker build -f ./container_coordinator/container_coordinator.dockerfile -t "container_coordinator:latest" . 
#docker run --rm -v /var/run/docker.sock:/var/run/docker.sock --name container_coordinator "container_coordinator:latest"
export COMPOSE_HTTP_TIMEOUT=230
docker-compose -f docker-compose-dev.yaml up --build --remove-orphans