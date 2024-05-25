#!/bin/bash
docker compose -f docker-compose-test.yaml stop -t 5
docker compose -f docker-compose-test.yaml down