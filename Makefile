.PHONY: pre-commit
pre-commit:
	@echo "Running pre-commit hooks..."
	@uv run pre-commit run --all-files
	@echo "pre-commit hooks completed."

.PHONY: tests
tests:
	@echo "Running tests..."
	@uv run pytest tests
	@echo "tests completed."

.PHONY: ruff
ruff:
	@echo "Running ruff..."
	@uv run ruff check .
	@echo "ruff completed."

.PHONY: install
install:
	@echo "Installing dependencies..."
	@uv sync --dev
	@echo "Dependencies installed."

.PHONY: extract
extract:
	@uv run python main_extract.py

.PHONY: login
login:
	@test -n "$(GCP_PROJECT_ID)" || (echo "GCP_PROJECT_ID not set (direnv allow?)" && exit 1)
	gcloud auth login
	gcloud auth application-default login
	gcloud auth application-default set-quota-project $(GCP_PROJECT_ID)

# make load URI=gs://pbk-raw-dev/stations/raw_data/<date>/<time>/data.json
.PHONY: load
load:
	@test -n "$(URI)" || (echo "usage: make load URI=gs://bucket/path/data.json" && exit 1)
	@uv run python main_load.py $(URI)
