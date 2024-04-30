FROM rabbitmq:3.9.16-management-alpine

RUN apk update && \
    apk add --no-cache python3 py3-pip && \
    rm -rf /var/cache/apk/*


RUN python3 -m pip install pika

COPY ./lib/middleware.py middleware.py
COPY ./lib/communications.py communications.py
COPY ./lib/serialization.py serialization.py
COPY ./query_coordinator/query_coordinator.py query_coordinator.py
COPY ./query_coordinator/query_coordinator_worker.py query_coordinator_worker.py

CMD ["python3", "query_coordinator_worker.py"]