.PHONY: bootstrap up down test test-python test-node grade-local fetch-phh ingest-sample-historical ingest-phh train-profiles simulate-demo simulate-context stream-live stream-catchup dashboard project-events

PYTHON ?= $(shell command -v python3 >/dev/null 2>&1 && echo python3 || echo python)
NODE_BIN ?= $(shell command -v node >/dev/null 2>&1 && echo node || command -v node.exe >/dev/null 2>&1 && echo node.exe || echo node)
PIP ?= $(PYTHON) -m pip

bootstrap:
	$(PIP) install -e .[dev,kafka,snowflake]
	npm install

up:
	docker compose up -d --build

down:
	docker compose down

test: test-python test-node

test-python:
	TMPDIR=$${TMPDIR:-/tmp} $(PYTHON) -m pytest

test-node:
	$(NODE_BIN) --test simulation/test/*.test.js

grade-local:
	@printf "\n"
	@printf "%s\n" "Starting the local grading stack. First build may take several minutes..."
	docker compose up -d --build --wait --wait-timeout 600
	@printf "\n"
	@printf "%s\n" "Open the application UI: http://localhost:3000/app"
	@printf "%s\n" "Streamlit dashboard:    http://localhost:8501"
	@printf "%s\n" "Airflow UI:             http://localhost:8080  (airflow / airflow)"
	@printf "%s\n" "Spark master UI:        http://localhost:8088"
	@printf "\n"

fetch-phh:
	docker compose exec airflow bash -lc 'python3 -m historical_ingest.fetch_phh --source "$${PHH_DATASET_SOURCE:-sample_data/historical/handhq_6max_sample.phhs}" --cache-dir "$${PHH_CACHE_DIR:-runtime/phh_cache}"'

ingest-sample-historical:
	docker compose exec airflow bash -lc 'python3 -m historical_ingest.handhq_backfill --input sample_data/historical/handhq_6max_sample.phhs --source-run-id sample_handhq_ingest --dataset handhq_sample --worker-count 1 --target-rows-per-chunk 1000 --write'

ingest-phh:
	docker compose exec airflow bash -lc 'python3 -m historical_ingest.handhq_backfill --input "$${PHH_DATASET_SOURCE:-sample_data/historical/handhq_6max_sample.phhs}" --cache-dir "$${PHH_CACHE_DIR:-runtime/phh_cache}" --source-run-id "$${HISTORICAL_SOURCE_RUN_ID:-historical_handhq_ingest}" --dataset "$${HISTORICAL_DATASET:-handhq_historical}" --worker-count "$${HANDHQ_WORKER_COUNT:-4}" --target-rows-per-chunk "$${HANDHQ_TARGET_ROWS_PER_CHUNK:-100000}" --write'

train-profiles:
	docker compose exec airflow bash -lc 'python3 -m profiles.build_features && python3 -m profiles.train_model'

simulate-demo:
	docker compose exec airflow bash -lc 'python3 -m simulator.run_simulation --hands 5 --seed 42 --context "balanced aggressive value bettor" --publish-kafka --no-backend-write --kafka-brokers kafka:29092 --actions-topic poker.actions --summaries-topic poker.hand_summaries'

simulate-context:
	$(NODE_BIN) simulation/scripts/simulate.js --baseUrl=http://localhost:3000 --hands=$${HANDS:-500} --seed=$${SEED:-42} --context="$${CONTEXT:-loose aggressive bluffer}"

stream-live:
	docker compose up -d spark-stream

stream-catchup:
	docker compose exec airflow bash -lc '/opt/spark/bin/spark-submit --master "$${SPARK_MASTER_URL:-local[*]}" --conf "spark.driver.memory=$${SPARK_DRIVER_MEMORY:-2g}" --conf "spark.driver.maxResultSize=$${SPARK_DRIVER_MAX_RESULT_SIZE:-1g}" --packages "$${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8}" jobs/stream_kafka.py --brokers kafka:29092 --topic poker.actions --summaries-topic poker.hand_summaries --events-topic poker.simulation_events --starting-offsets earliest --checkpoint-path "$${STREAM_CHECKPOINT_PATH:-/tmp/poker_platform_runtime/stream_checkpoints/poker}" --available-now'

dashboard:
	docker compose up -d dashboard

project-events:
	docker compose up -d simulation-event-projector
