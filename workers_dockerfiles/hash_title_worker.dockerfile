FROM python:3.9.7-slim

RUN python3 -m pip install pika
RUN python3 -m pip install textblob

COPY ./lib/serialization.py serialization.py
COPY ./lib/middleware.py middleware.py
COPY ./lib/filters.py filters.py
COPY ./lib/workers.py workers.py
COPY ./lib/healtchecking.py healtchecking.py

COPY ./workers/hash_title_worker.py hash_title_worker.py

CMD ["python3", "hash_title_worker.py"]
