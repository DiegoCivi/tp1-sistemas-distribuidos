FROM python:3.9.7-slim

RUN apt-get update
RUN python3 -m pip install docker
#COPY ./workers_dockerfiles workers_dockerfiles
#COPY ./workers workers
COPY ./lib/communications.py communications.py
COPY ./lib/healthchecking.py healthchecking.py
COPY ./container_coordinator/container_coordinator.py container_coordinator.py
COPY ./containers_list.config containers_list.config
#COPY ./lib/ lib



CMD ["python3", "container_coordinator.py"]