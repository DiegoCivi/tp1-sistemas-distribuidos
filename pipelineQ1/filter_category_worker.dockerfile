FROM rabbitmq:latest

RUN apk update && \
    apk add --no-cache python3 py3-pip && \
    rm -rf /var/cache/apk/*

RUN python3 -m pip install pika

COPY ./lib/serialization.py serialization.py
COPY ./lib/middleware.py middleware.py
COPY ./lib/filters.py filters.py

COPY ./workers/filter_category_worker.py filter_category_worker.py

CMD ["python3", "filter_category_worker.py"]
