# Common commands. Run `make help` to list them.
VENV   ?= .venv
PY      = $(VENV)/bin/python
BIN     = $(VENV)/bin
DBTARGS = --project-dir dbt_project --profiles-dir dbt_project
DAYS   ?= 7

.DEFAULT_GOAL := help
.PHONY: help setup lint format test run report dbt-build dbt-docs airflow-up airflow-down clean

help:        ## list commands
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-13s %s\n", $$1, $$2}'

setup:       ## create virtualenv and install dependencies
	python3 -m venv $(VENV)
	$(BIN)/pip install -r requirements-dev.txt

lint:        ## static checks
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .

format:      ## auto-format code
	$(BIN)/ruff format .
	$(BIN)/ruff check . --fix

test:        ## python unit tests
	$(BIN)/pytest -q

run:         ## run the full pipeline day by day (DAYS=7 by default), from scratch
	PATH=$(BIN):$$PATH $(PY) -m scripts.run_pipeline --days $(DAYS) --fresh

report:      ## rebuild the HTML quality report
	$(PY) -m scripts.build_quality_report

dbt-build:   ## run dbt on whatever is already loaded
	$(BIN)/dbt build $(DBTARGS)

dbt-docs:    ## generate and serve dbt docs with lineage graph at http://localhost:8081
	$(BIN)/dbt docs generate $(DBTARGS)
	$(BIN)/dbt docs serve $(DBTARGS) --port 8081

airflow-up:  ## start Airflow in Docker at http://localhost:8080 (user admin, no password in local mode)
	docker compose up --build -d

airflow-down: ## stop Airflow
	docker compose down

clean:       ## delete generated data, reports and dbt artifacts
	rm -rf data reports dbt_project/target dbt_project/logs logs
