from .fault_tolerance import (
    CHAOS_EXCLUDE,
    CHAOS_INTERVAL,
    CHAOS_KILLS_MAX,
    CHAOS_KILLS_MIN,
    CHAOS_START_DELAY,
)


def gen_chaos() -> str:
    # Disabled by default; arm with CHAOS_ENABLED=1. Defaults live in fault_tolerance.py.
    return f"""\n
# === chaos monkey (disabled by default; set CHAOS_ENABLED=1 to arm) ===

  chaos:
    build:
      context: ./src/
      dockerfile: chaos/Dockerfile
    container_name: chaos
    environment:
      - CHAOS_ENABLED=${{CHAOS_ENABLED:-0}}
      - CHAOS_INTERVAL=${{CHAOS_INTERVAL:-{CHAOS_INTERVAL}}}
      - CHAOS_KILLS_MIN=${{CHAOS_KILLS_MIN:-{CHAOS_KILLS_MIN}}}
      - CHAOS_KILLS_MAX=${{CHAOS_KILLS_MAX:-{CHAOS_KILLS_MAX}}}
      - CHAOS_START_DELAY=${{CHAOS_START_DELAY:-{CHAOS_START_DELAY}}}
      - CHAOS_TARGETS=${{CHAOS_TARGETS:-}}
      - CHAOS_EXCLUDE=${{CHAOS_EXCLUDE:-{CHAOS_EXCLUDE}}}
      - CHAOS_SEED=${{CHAOS_SEED:-}}
      - LOGGING_LEVEL=INFO
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock"""
