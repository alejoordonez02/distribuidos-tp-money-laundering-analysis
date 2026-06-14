SHELL := /bin/bash
PWD := $(shell pwd)
PYTHON_PM := /bin/uv
COMPOSE_FILE := docker-compose.yaml
COMPOSE := docker compose -f $(COMPOSE_FILE)
RABBIT_CONTAINER := rabbitmq
SCRIPTS_DIR := scripts

.PHONY: help gen_input_output gen_compose up stop_server down logs test test_ft report demo supervisor chaos chaos_stop resilience

help:
	@echo '* opciones: help (esto) - gen_input_output - gen_compose - up - stop_server - down - logs - test - test_ft - supervisor - chaos - chaos_stop - report - demo'
	@echo '* los datasets a usar se configuran en `scripts/cfg.py`, hay que tenerlos bajados en `datasets/`'
	@echo '* para los targets que se corren en python se usa `uv`. Hay que tenerlo instalado'

gen_input_output:
	mkdir -p test/expected_responses
	PYTHONPATH=src uv run $(SCRIPTS_DIR)/gen_input_output.py # TODO: este script hay q limpiarlo después

gen_compose:
	uv run -m $(SCRIPTS_DIR).gen_compose.gen_compose $(COMPOSE_FILE)

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

# fault-tolerance e2e: crash each controller at each crash point, verify recovery
test_ft:
	mkdir -p responses tmp/ft_run
	PYTHONPATH=src uv run $(SCRIPTS_DIR)/ft_e2e.py

# attach to the supervisor's live dashboard (detach with Ctrl-P Ctrl-Q)
supervisor:
	docker attach supervisor

# arm the chaos monkey on a running cluster and stream what it kills
# (Ctrl-C stops watching the logs; the chaos container keeps running)
chaos:
	CHAOS_ENABLED=1 $(COMPOSE) up -d --force-recreate --no-deps chaos
	$(COMPOSE) logs -f chaos

# disarm the chaos monkey
chaos_stop:
	CHAOS_ENABLED=0 $(COMPOSE) up -d --force-recreate --no-deps chaos

# end-to-end resilience demo under chaos, then verify 5/5 (DATASET=small|medium|large)
resilience:
	bash $(SCRIPTS_DIR)/resilience_demo.sh

report:
	cd doc/informe && ./make_report
	cd ../..

demo:
	mkdir -p demo/files
	PYTHONPATH=test uv run $(SCRIPTS_DIR)/gen_demo_files.py
