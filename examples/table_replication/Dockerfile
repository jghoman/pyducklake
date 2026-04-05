FROM python:3.12-slim
WORKDIR /app

COPY . /pyducklake
RUN pip install --no-cache-dir /pyducklake

COPY examples/table_replication/ /app/
