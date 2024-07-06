.PHONY: test build run

test:
	cd etl && pytest

build:
	docker-compose build

run:
	docker-compose up etl