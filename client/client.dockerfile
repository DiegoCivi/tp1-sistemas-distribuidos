FROM python:latest

COPY ./lib/communications.py communications.py
COPY ./client/files.py files.py
COPY ./client/client.py client.py
COPY ./lib/serialization.py serialization.py

CMD ["python3", "client.py"]