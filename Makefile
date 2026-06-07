SHELL := /bin/bash
PWD := $(shell pwd)
PYTHON_PM := /bin/uv
COMPOSE := docker compose -f test.yaml -f docker-compose.clients.yaml
RABBIT_CONTAINER := rabbitmq

.PHONY: help gen_input_output gen_compose up stop_server down logs test report demo

help:
	@echo '* opciones: help (esto) - gen_input_output - gen_compose - up - stop_server - down - logs - test - report'
	@echo '* para up tienen que tener bajado los datasets LI-Small y dejarlos en `datasets/`'
	@echo '  no los pusheé porque son demasiado grandes hasta comprimidos'
	@echo '* para los targets que se corren en python se usa `uv`. Hay que tenerlo instalado'
	@echo '* NCLIENTS se configura en test/cfg.py — gen_compose regenera docker-compose.clients.yaml'

gen_input_output:
	mkdir -p test/expected_responses
	PYTHONPATH=src uv run test/gen_input_output.py # TODO: este script hay q limpiarlo después

gen_compose:
	PYTHONPATH=test uv run -m scripts.gen_compose.gen_compose

up: gen_compose
	mkdir -p responses
	$(COMPOSE) up --build --remove-orphans --detach
	$(COMPOSE) logs --follow

stop_server: gen_compose
	NON_RABBIT=$$($(COMPOSE) ps -q | grep -v $$(docker ps -q -f "name=$(RABBIT_CONTAINER)")); \
	if [ -n "$$NON_RABBIT" ]; then \
		docker stop $$NON_RABBIT -t 5; \
	fi

down: gen_compose
	$(COMPOSE) stop -t 5
	$(COMPOSE) down

logs: gen_compose
	$(COMPOSE) logs

test:
	uv run pytest

report:
	cd doc/informe && ./make_report
	cd ../..

demo:
	mkdir -p demo/files
	PYTHONPATH=test uv run test/gen_demo_files.py
