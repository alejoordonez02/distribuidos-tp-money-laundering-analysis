SHELL := /bin/bash
PWD := $(shell pwd)
PYTHON_PM := /bin/uv
COMPOSE_FILE := docker-compose.yaml
COMPOSE := docker compose -f $(COMPOSE_FILE)
RABBIT_CONTAINER := rabbitmq
SCRIPTS_DIR := scripts

.PHONY: help gen_input_output gen_compose up stop_server down logs test test_ft test_ft_client scalability_test performance_vs_ft perf_plots report demo supervisor chaos chaos_stop

help:
	@echo '* opciones: help (esto) - gen_input_output - gen_compose - up - stop_server - down - logs - test - test_ft - test_ft_client - report - demo - supervisor - chaos - chaos_stop'
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

# scalability e2e: run several ring topologies against each dataset tier, verify each
# still matches the oracle. Select with SCALE_TOPOS / SCALE_TIERS / SCALE_REPEAT.
scalability_test:
	mkdir -p responses tmp/scalability
	PYTHONPATH=src uv run $(SCRIPTS_DIR)/scalability_e2e.py
# convenience shortcut: the SAME ft e2e, filtered to just the client-crash points
# (drop a client mid-stream, verify the pipeline purges its partial data and a fresh
# client still gets a correct result). Plain `make test_ft` already covers these too.
test_ft_client:
	mkdir -p responses tmp/ft_run
	FT_ONLY_POINTS=client_mid_transactions,client_mid_accounts,client_after_eof \
		PYTHONPATH=src uv run $(SCRIPTS_DIR)/ft_e2e.py

# fault-tolerance vs performance benchmark: proportional chaos + checkpoint sweeps on
# the min2 topology; dumps tmp/ft_perf/results.csv and regenerates the report figures.
performance_vs_ft:
	mkdir -p responses tmp/ft_perf
	PYTHONPATH=src uv run $(SCRIPTS_DIR)/ft_perf_bench.py

# re-render the report figures from an existing results.csv (no cluster)
perf_plots:
	uv run --with matplotlib --with numpy $(SCRIPTS_DIR)/plot_ft_perf.py

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

report:
	cd doc/informe && ./make_report
	cd ../..

demo:
	mkdir -p demo/files
	PYTHONPATH=test uv run $(SCRIPTS_DIR)/gen_demo_files.py
