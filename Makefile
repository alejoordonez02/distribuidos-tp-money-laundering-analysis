SHELL := /bin/bash
PWD := $(shell pwd)
PYTHON_PM := /bin/uv

.PHONY: help gen_input_output up down logs test report 

help:
	@echo '* opciones: help (esto) - gen_input_output - up - down - logs - test - report'
	@echo '* para up tienen que tener bajado los datasets LI-Small y dejarlos en `datasets/`'
	@echo '  no los pusheé porque son demasiado grandes hasta comprimidos'
	@echo '* para los targets que se corren en python se usa `uv`. Hay que tenerlo instalado'

gen_input_output:
	mkdir -p test/expected_responses
	PYTHONPATH=src uv run test/gen_input_output.py # TODO: este script hay q limpiarlo después

up:
	mkdir -p responses
	docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down

logs:
	docker compose -f docker-compose.yaml logs

test:
	uv run pytest

report:
	cd doc/informe && ./make_report
	cd ../..
