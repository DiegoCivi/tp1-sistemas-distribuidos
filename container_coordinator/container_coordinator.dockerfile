FROM python:3.9.7-slim

RUN apt-get update
RUN apt-get install -y docker.io

#COPY ./workers_dockerfiles workers_dockerfiles
#COPY ./workers workers
COPY ./lib/communications.py communications.py
COPY ./container_coordinator/container_coordinator.py container_coordinator.py
#COPY ./lib/ lib



CMD ["python3", "container_coordinator.py"]