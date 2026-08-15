FROM python:3.14-slim@sha256:71b358f8bff55413f4a6b95af80acb07ab97b5636cd3b869f35c3680d31d1650
WORKDIR /app

COPY . /pyducklake
RUN pip install --no-cache-dir /pyducklake deltalake

COPY examples/delta_replication/ /app/
