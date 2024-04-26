FROM rabbitmq:3.9.16-management-alpine

RUN apk update && \
    apk add --no-cache python3 py3-pip && \
    rm -rf /var/cache/apk/*


RUN python3 -m pip install pika

COPY ./lib/serialization.py serialization.py
COPY ./lib/middleware.py middleware.py
COPY ./lib/filters.py filters.py

COPY ./workers/filter_title_worker.py filter_title_worker.py

CMD ["python3", "filter_title_worker.py"]
