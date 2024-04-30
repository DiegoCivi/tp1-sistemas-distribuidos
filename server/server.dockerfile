FROM rabbitmq:3.9.16-management-alpine

RUN apk update && \
    apk add --no-cache python3 py3-pip && \
    rm -rf /var/cache/apk/*


RUN python3 -m pip install pika

COPY ./lib/middleware.py middleware.py
COPY ./lib/communications.py communications.py
COPY ./server/server.py server.py

CMD ["python3", "server.py"]