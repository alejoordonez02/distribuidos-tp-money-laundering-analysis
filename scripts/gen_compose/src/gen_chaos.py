def gen_chaos() -> str:
    # Disabled by default so normal runs are untouched; arm it by setting
    # CHAOS_ENABLED=1 (and tune interval/kills/exclude) before `make up`.
    return """\n
# === chaos monkey (disabled by default; set CHAOS_ENABLED=1 to arm) ===

  chaos:
    build:
      context: ./src/
      dockerfile: chaos/Dockerfile
    container_name: chaos
    environment:
      - CHAOS_ENABLED=${CHAOS_ENABLED:-0}
      - CHAOS_INTERVAL=${CHAOS_INTERVAL:-4}
      - CHAOS_KILLS_MIN=${CHAOS_KILLS_MIN:-1}
      - CHAOS_KILLS_MAX=${CHAOS_KILLS_MAX:-8}
      - CHAOS_START_DELAY=${CHAOS_START_DELAY:-5}
      - CHAOS_TARGETS=${CHAOS_TARGETS:-}
      - CHAOS_EXCLUDE=${CHAOS_EXCLUDE:-rabbitmq,supervisor,gateway,chaos}
      - CHAOS_SEED=${CHAOS_SEED:-}
      - LOGGING_LEVEL=INFO
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock"""
