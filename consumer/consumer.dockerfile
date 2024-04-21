FROM rabbitmq:latest

RUN apk update && \
    apk add --no-cache python3 py3-pip && \
    rm -rf /var/cache/apk/*

RUN python3 -m pip install pika

COPY ./consumer/consumer.py consumer.py
COPY ./lib/middleware.py middleware.py

CMD ["python3", "consumer.py"]