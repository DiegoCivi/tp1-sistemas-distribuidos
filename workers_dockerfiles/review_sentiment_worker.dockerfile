FROM rabbitmq:3.9.16-management-alpine

RUN apk update && \
    apk add --no-cache python3 py3-pip gcc python3-dev musl-dev && \
    rm -rf /var/cache/apk/*

RUN python3 -m pip install pika
RUN python3 -m pip install textblob

COPY ./lib/serialization.py serialization.py
COPY ./lib/middleware.py middleware.py
COPY ./lib/filters.py filters.py

COPY ./workers/review_sentiment_worker.py review_sentiment_worker.py

CMD ["python3", "review_sentiment_worker.py"]
