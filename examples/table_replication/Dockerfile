FROM python:3.12-slim@sha256:5072b08ad74609c5329ab4085a96dfa873de565fb4751a4cfcd7dcc427661df0
WORKDIR /app

COPY . /pyducklake
RUN pip install --no-cache-dir /pyducklake

COPY examples/table_replication/ /app/
