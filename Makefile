.PHONY: up down logs topics test lint clean

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

topics:
	docker compose up kafka-init

test:
	uv run pytest

lint:
	uv run ruff check src tests

clean:
	docker compose down -v --remove-orphans
