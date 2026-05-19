SHELL := /bin/bash
PWD := $(shell pwd)
PYTHON_PM := /bin/uv

.PHONY: help up down logs test report 

help:
	@echo '* opciones: help (esto) - up - down - logs - test - report'
	@echo '* para up tienen que tener bajado los datasets LI-Small y dejarlos en `datasets/`'
	@echo '  no los pusheé porque son demasiado grandes hasta comprimidos'
	@echo '* para los targets que se corren en python se usa `uv`. Hay que tenerlo instalado'

up:
	@echo 'generating client datasets and expected responses'
	uv run test/gen_input_output.py # TODO: este script hay q limpiarlo después
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
