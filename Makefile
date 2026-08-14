.PHONY: setup download kafka-up kafka-down produce stream snowflake-load dbt-build dbt-docs

PY := .venv/bin/python
DBT := .venv/bin/dbt
DAY ?= 2024-01-15

# Only targets whose code actually exists are listed. airflow-up and
# backfill-test arrive in week 4 -- a Makefile of stub targets that fail on
# every invocation is worse documentation than no target at all.

setup:
	python3 -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt

download:
	$(PY) ingest/download.py --day $(DAY)

kafka-up:
	docker compose up -d

kafka-down:
	docker compose down

produce:
	$(PY) ingest/producer.py --input 'data/raw/$(DAY)-*.json.gz'

# Reads S3_BUCKET from .env at recipe time rather than hardcoding it, so this
# target works from a fresh clone once .env is filled in, not just on this
# machine.
stream:
	set -a && . ./.env && set +a && \
	$(PY) spark/stream_to_delta.py \
		--output s3a://$$S3_BUCKET/delta/events \
		--checkpoint s3a://$$S3_BUCKET/checkpoints/events

# Order matters: export_for_snowflake.py must run before copy_into.sql on
# every load, not just the first one -- see spark/export_for_snowflake.py for
# why COPY INTO against the raw Delta path double-counts otherwise.
snowflake-load:
	$(PY) snowflake/run_sql.py snowflake/setup.sql
	$(PY) spark/export_for_snowflake.py
	$(PY) snowflake/run_sql.py snowflake/copy_into.sql

dbt-build:
	cd dbt_project && ../$(DBT) build

dbt-docs:
	cd dbt_project && ../$(DBT) docs generate && ../$(DBT) docs serve --port 8081
