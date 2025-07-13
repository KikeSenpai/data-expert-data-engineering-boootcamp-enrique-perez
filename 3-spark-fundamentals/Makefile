.PHONY: up
up:  ## Start the Spark cluster
	@echo "Starting Spark cluster..."
	@docker compose up -d

.PHONY: down
down: ## Stop the Spark cluster
	@echo "Stopping Spark cluster..."
	@docker compose down

.PHONY: jupyter
jupyter: ## Open Jupyter Notebook
	@echo "Opening Jupyter Notebook..."
	@open http://localhost:8888

.PHONY: test
test: ## Run unit tests
	@echo "$(OK_COLOR)==> Running unit tests...$(NO_COLOR)"
	@uv run pytest tests/unit -vv --color=yes

.PHONY: setup
setup: ## Setup local development environment
	@echo "$(OK_COLOR)==> Setting up development environment...$(NO_COLOR)"
	@uv sync --all-extras --dev
