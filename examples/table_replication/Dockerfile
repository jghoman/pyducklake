FROM python:3.14-slim@sha256:bc389f7dfcb21413e72a28f491985326994795e34d2b86c8ae2f417b4e7818aa
WORKDIR /app

COPY . /pyducklake
RUN pip install --no-cache-dir /pyducklake

COPY examples/table_replication/ /app/
