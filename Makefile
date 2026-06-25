SHELL := /bin/bash
PWD := $(shell pwd)
PYTHON_PM := /bin/uv
COMPOSE_FILE := docker-compose.yaml
COMPOSE := docker compose -f $(COMPOSE_FILE)
RABBIT_CONTAINER := rabbitmq
SUPERVISOR_PREFIX := supervisor_
SCRIPTS_DIR := scripts

.PHONY: help gen_input_output gen_compose up stop_server down logs test test_ft test_ft_client scalability_test performance_vs_ft perf_plots report demo supervisor chaos chaos_stop nodes kill kill_prefix dead revive revive_prefix

help:
	@echo '* opciones: help (esto) - gen_input_output - gen_compose - up - stop_server - down - logs - test - test_ft - test_ft_client - report - demo - supervisor - chaos - chaos_stop - nodes - kill - kill_prefix - dead - revive - revive_prefix'
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

stop_server: gen_compose
	NON_RABBIT=$$($(COMPOSE) ps -q | grep -v $$(docker ps -q -f "name=$(RABBIT_CONTAINER)")); \
	if [ -n "$$NON_RABBIT" ]; then \
		docker stop $$NON_RABBIT -t 5; \
	fi

down: gen_compose
	$(COMPOSE) stop -t 5
	$(COMPOSE) down

logs: gen_compose
	SERVICES=$$($(COMPOSE) config --services | grep -v '^$(SUPERVISOR_PREFIX)'); \
	if [ -n "$$SERVICES" ]; then \
		$(COMPOSE) logs -f $$SERVICES; \
	fi

supervisor: gen_compose
	SERVICES=$$($(COMPOSE) config --services | grep '^$(SUPERVISOR_PREFIX)'); \
	if [ -n "$$SERVICES" ]; then \
		$(COMPOSE) logs -f $$SERVICES; \
	fi

revive_supervisors:
	SERVICES=$$($(COMPOSE) config --services | grep '$(SUPERVISOR_PREFIX)'); \
	if [ -n "$$SERVICES" ]; then \
		$(COMPOSE) start $$SERVICES; \
	fi

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
	uv run --with matplotlib --with numpy --with adjustText $(SCRIPTS_DIR)/plot_ft_perf.py

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

# --- inyección manual de fallas: matar nodos a mano para probar FT sin el chaos monkey ---
# listar nodos vivos (qué se puede matar): make nodes
# matar un nodo puntual:                   make kill NODE=join_0
# matar varios (separados por coma):       make kill NODE=join_0,uc3_merge_1
# matar un grupo entero por prefijo:       make kill_prefix PREFIX=join   (mata join_0, join_1, ...)
nodes:
	@docker ps --format '{{.Names}}' | grep -v -x '$(RABBIT_CONTAINER)' | sort

kill:
	@if [ -z "$(NODE)" ]; then \
		echo 'uso: make kill NODE=<nombre>[,<nombre>...]   (ver nombres con: make nodes)'; \
		exit 1; \
	fi
	@for n in $$(echo '$(NODE)' | tr ',' ' '); do \
		if docker ps --format '{{.Names}}' | grep -q -x "$$n"; then \
			docker kill "$$n" >/dev/null && echo "killed: $$n"; \
		else \
			echo "no está corriendo (skip): $$n"; \
		fi; \
	done

# mata todos los contenedores vivos cuyo nombre empieza con PREFIX (rabbitmq queda protegido;
# para matarlo usá `make kill NODE=$(RABBIT_CONTAINER)` explícitamente)
kill_prefix:
	@if [ -z "$(PREFIX)" ]; then \
		echo 'uso: make kill_prefix PREFIX=<prefijo>   (ej: PREFIX=uc4 mata uc4_*)'; \
		exit 1; \
	fi
	@VICTIMS=$$(docker ps --format '{{.Names}}' | grep -E "^$(PREFIX)" | grep -v -x '$(RABBIT_CONTAINER)'); \
	if [ -z "$$VICTIMS" ]; then \
		echo "no hay contenedores vivos con prefijo '$(PREFIX)'"; \
		exit 0; \
	fi; \
	for n in $$VICTIMS; do docker kill "$$n" >/dev/null && echo "killed: $$n"; done

# --- revivir nodos caídos a mano (docker start) — espejo de kill ---
# listar nodos caídos:              make dead
# revivir uno o varios (coma):      make revive NODE=join_0,uc3_merge_1
# revivir un grupo por prefijo:     make revive_prefix PREFIX=join
dead:
	@docker ps -a --filter status=exited --format '{{.Names}}' | grep -v -x '$(RABBIT_CONTAINER)' | sort

revive:
	@if [ -z "$(NODE)" ]; then \
		echo 'uso: make revive NODE=<nombre>[,<nombre>...]   (ver caídos con: make dead)'; \
		exit 1; \
	fi
	@for n in $$(echo '$(NODE)' | tr ',' ' '); do \
		if docker ps --format '{{.Names}}' | grep -q -x "$$n"; then \
			echo "ya está corriendo (skip): $$n"; \
		elif docker ps -a --format '{{.Names}}' | grep -q -x "$$n"; then \
			docker start "$$n" >/dev/null && echo "revived: $$n"; \
		else \
			echo "no existe (skip): $$n"; \
		fi; \
	done

# revive todos los contenedores CAÍDOS cuyo nombre empieza con PREFIX
revive_prefix:
	@if [ -z "$(PREFIX)" ]; then \
		echo 'uso: make revive_prefix PREFIX=<prefijo>   (ej: PREFIX=uc4 revive uc4_*)'; \
		exit 1; \
	fi
	@TARGETS=$$(docker ps -a --filter status=exited --format '{{.Names}}' | grep -E "^$(PREFIX)"); \
	if [ -z "$$TARGETS" ]; then \
		echo "no hay contenedores caídos con prefijo '$(PREFIX)'"; \
		exit 0; \
	fi; \
	for n in $$TARGETS; do docker start "$$n" >/dev/null && echo "revived: $$n"; done
