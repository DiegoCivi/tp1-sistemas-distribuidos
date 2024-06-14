FROM python:3.9.7-slim

RUN python3 -m pip install pika
RUN python3 -m pip install textblob
RUN python3 -m pip install docker

COPY ./lib/serialization.py serialization.py
COPY ./lib/middleware.py middleware.py
COPY ./lib/filters.py filters.py
COPY ./lib/workers.py workers.py
COPY ./lib/healthchecking.py healthchecking.py
COPY ./lib/communications.py communications.py

COPY ./workers/filter_review_quantity_worker.py filter_review_quantity_worker.py

CMD ["python3", "filter_review_quantity_worker.py"]
