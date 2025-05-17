# Makefile for my app

# Default variables
APP_NAME=firm_GDP.py
ENV_NAME=venv

# Default target
.PHONY: run
run:
	streamlit run $(APP_NAME)


.PHONY: setup run

setup:
	python3 -m venv $(ENV_NAME)
	. $(ENV_NAME)/bin/activate && pip install -r requirements.txt


.PHONY: freeze
freeze:
	pip freeze > requirements.txt

.PHONY: format
format:
	black .

.PHONY: lint
lint:
	flake8 .

.PHONY: test
test:
	pytest

.PHONY: clean
clean:
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type d -name "*.pytest_cache" -exec rm -r {} +

.PHONY: help
help:
	@echo "Makefile commands:"
	@echo "  make run       - Run the Streamlit app"
	@echo "  make setup     - Setup virtual environment and install dependencies"
	@echo "  make freeze    - Export dependencies to requirements.txt"
	@echo "  make format    - Format code using black"
	@echo "  make lint      - Lint code using flake8"
	@echo "  make test      - Run tests with pytest"
	@echo "  make clean     - Remove cache files"

