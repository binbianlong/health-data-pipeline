SHELL := /bin/sh
.DEFAULT_GOAL := help

UV ?= uv
TF ?= terraform
PROJECT_ID ?=
REGION ?= us-central1
TF_STATE_BUCKET ?=

.PHONY: help install lock format lint typecheck test audit check docker-test docker-runtime \
	terraform-fmt terraform-validate setup-code subscriber-upsert notion-configure

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\\n\\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-22s %s\\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install locked development dependencies
	$(UV) sync --frozen --all-groups

lock: ## Refresh uv.lock after reviewing dependency changes
	$(UV) lock

format: ## Format Python and Terraform files
	$(UV) run ruff format .
	$(UV) run ruff check --fix .
	$(TF) fmt -recursive infra

lint: ## Run Python lint and formatting checks
	$(UV) run ruff format --check .
	$(UV) run ruff check .

typecheck: ## Run strict static type checks
	$(UV) run mypy src tests

test: ## Run tests with coverage
	$(UV) run pytest

audit: ## Audit locked Python dependencies
	@mkdir -p .cache
	$(UV) export --frozen --no-dev --no-emit-project --format requirements-txt --output-file .cache/audit-requirements.txt
	$(UV) run pip-audit --requirement .cache/audit-requirements.txt --disable-pip

check: lint typecheck test audit terraform-validate ## Run all local checks

docker-test: ## Explicitly build and execute the Docker test stage
	docker build --target test --tag health-data-pipeline:test .

docker-runtime: ## Build the production runtime image
	docker build --target runtime --tag health-data-pipeline:local .

terraform-fmt: ## Check Terraform formatting
	$(TF) fmt -check -recursive infra

terraform-validate: terraform-fmt ## Initialize without a backend and validate both roots
	$(TF) -chdir=infra/bootstrap init -backend=false
	$(TF) -chdir=infra/bootstrap validate
	$(TF) -chdir=infra/main init -backend=false
	$(TF) -chdir=infra/main validate

setup-code: ## Generate a one-time setup code through the application CLI
	$(UV) run python -m health_data_pipeline.cli setup-code

subscriber-upsert: ## Create or update the automatic Google Health webhook subscriber
	$(UV) run python -m health_data_pipeline.cli subscriber-upsert

notion-configure: ## Create or repair the required Notion data source properties
	$(UV) run python -m health_data_pipeline.cli notion-configure
