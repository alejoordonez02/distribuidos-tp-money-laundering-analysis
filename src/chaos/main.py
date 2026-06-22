import logging
import os
import random
import subprocess
import time

_INFRA_PREFIXES = ("client",)


def select_victims(candidates, exclude, kills, rng):
    """Pure target selection: drop excluded names and infra, then randomly sample
    up to `kills` victims. Kept separate from Docker so it is easy to test."""
    pool = sorted(c for c in candidates if c not in exclude and not c.startswith(_INFRA_PREFIXES))
    if not pool:
        return []
    return rng.sample(pool, min(kills, len(pool)))


def _running_containers():
    out = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        capture_output=True, text=True, timeout=15,
    )
    return out.stdout.split()


def _kill(name):
    subprocess.run(["docker", "kill", name], capture_output=True, timeout=15)


def main():
    logging.basicConfig(level=os.getenv("LOGGING_LEVEL", "INFO"))
    if os.getenv("CHAOS_ENABLED", "0") != "1":
        logging.info("chaos monkey disabled (set CHAOS_ENABLED=1 to arm it)")
        return

    interval = float(os.getenv("CHAOS_INTERVAL", "30"))
    kmin = int(os.getenv("CHAOS_KILLS_MIN", os.getenv("CHAOS_KILLS", "1")))
    kmax = int(os.getenv("CHAOS_KILLS_MAX", os.getenv("CHAOS_KILLS", str(kmin))))
    start_delay = float(os.getenv("CHAOS_START_DELAY", "5"))
    seed = os.getenv("CHAOS_SEED")
    rng = random.Random(int(seed)) if seed else random.Random()
    # an explicit target list means directed mode; empty means discover live containers
    explicit = [t for t in os.getenv("CHAOS_TARGETS", "").split(",") if t]
    exclude = set(
        filter(None, os.getenv("CHAOS_EXCLUDE", "rabbitmq,supervisor,gateway,chaos").split(","))
    )

    logging.warning(
        "chaos monkey armed: interval=%ss kills=%s-%s targets=%s exclude=%s",
        interval, kmin, kmax, explicit or "auto", sorted(exclude),
    )
    time.sleep(start_delay)
    while True:
        candidates = explicit or _running_containers()
        victims = select_victims(candidates, exclude, rng.randint(kmin, kmax), rng)
        for v in victims:
            _kill(v)
        if victims:
            logging.warning("chaos: KILLED %d node(s) -> %s", len(victims), ", ".join(victims))
        time.sleep(interval)


if __name__ == "__main__":
    main()
