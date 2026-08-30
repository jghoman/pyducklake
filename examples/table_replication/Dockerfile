FROM python:3.14-slim@sha256:cae66f2ef0ec51a9891263eeee7f987dacf0a9879e8aa9353d5606e0530619a5
WORKDIR /app

COPY . /pyducklake
RUN pip install --no-cache-dir /pyducklake

COPY examples/table_replication/ /app/
