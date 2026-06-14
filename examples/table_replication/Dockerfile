FROM python:3.14-slim@sha256:44dd04494ee8f3b538294360e7c4b3acb87c8268e4d0a4828a6500b1eff50061
WORKDIR /app

COPY . /pyducklake
RUN pip install --no-cache-dir /pyducklake

COPY examples/table_replication/ /app/
