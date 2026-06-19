"""Shared cluster-driving helpers for the fault-tolerance and scalability e2e tests.

Both tests bring the cluster up once, run fresh clients through it, detect stalls by queue
progress and drain between clients; keeping that here means they read docker state the same
way. These scripts live in scripts/ and run as files, so a sibling `from ft_common import`
resolves directly.
"""

import os
import subprocess
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPOSE = "docker-compose.yaml"


def run(cmd, check=False, capture=False, timeout=None):
    """Run a shell command from the repo root, returning a failed result on timeout
    instead of raising."""
    try:
        return subprocess.run(
            cmd, shell=True, cwd=ROOT, check=check,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.STDOUT if capture else None,
            text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="")


def out(cmd, timeout=None):
    return run(cmd, capture=True, timeout=timeout).stdout.strip()


def _idx(name):
    tail = name.rsplit("_", 1)[1] if "_" in name else ""
    return int(tail) if tail.isdigit() else -1


def discover_controllers():
    """Every controller in the compose as [(type, [replica names], count)], excluding the
    broker, gateway, supervisor, chaos and clients."""
    services = out(f"docker compose -f {COMPOSE} config --services").splitlines()
    services = [s.strip() for s in services if s.strip()]
    infra = {"rabbitmq", "gateway", "supervisor", "chaos"}
    groups: dict[str, list[str]] = {}
    for name in sorted(services):
        if name in infra or name.startswith("client"):
            continue
        type_ = name
        if "_" in name and name.rsplit("_", 1)[1].isdigit():
            type_ = name.rsplit("_", 1)[0]
        groups.setdefault(type_, []).append(name)
    return [(t, sorted(names, key=_idx), len(names)) for t, names in sorted(groups.items())]


def non_client_services():
    services = out(f"docker compose -f {COMPOSE} config --services").splitlines()
    return [s.strip() for s in services if s.strip() and not s.strip().startswith("client")]


def clean_all_state():
    """Wipe every node's persisted (root-owned, in-container) state so a run starts from
    zero instead of restoring stale checkpoints."""
    run(f'docker run --rm -v "{os.path.join(ROOT, "state")}:/state" alpine '
        "find /state -mindepth 1 -delete", capture=True)


def compose_up_cluster(extra_env=""):
    """Bring up the broker, gateway, supervisor, chaos and every controller once, without
    any client. `extra_env` is a prefix of VAR=val pairs for the compose invocation."""
    svcs = " ".join(non_client_services())
    run(f"{extra_env} docker compose -f {COMPOSE} up -d --remove-orphans {svcs}", capture=True)


def recreate_client(client="client_0"):
    """Run one fresh client (new client_id + producer_id) through the live cluster."""
    run(f"docker compose -f {COMPOSE} up -d --force-recreate --no-deps {client}", capture=True)


def clear_responses():
    resp = os.path.join(ROOT, "responses")
    if os.path.isdir(resp):
        for f in os.listdir(resp):
            if f.endswith(".csv"):
                os.remove(os.path.join(resp, f))


def node_running(node):
    return out(f'docker inspect -f "{{{{.State.Running}}}}" {node}') == "true"


def clients_running():
    return bool(out('docker ps --filter "name=client_" --filter "status=running" -q'))


def queue_total():
    """Total in-flight messages across all queues, or None when the broker is too busy to
    answer (a missing reading is never treated as a stall)."""
    r = run("docker exec rabbitmq rabbitmqctl list_queues messages --quiet",
            capture=True, timeout=15)
    nums = [int(tok) for tok in r.stdout.split() if tok.isdigit()]
    return sum(nums) if nums else None


def client_exit_codes():
    names = out('docker ps -a --filter "name=client_" --format "{{.Names}}"').split()
    return [out(f'docker inspect -f "{{{{.State.ExitCode}}}}" {n}') for n in names]


def verify():
    """Run the per-UC oracle comparison, returning (ok, last_pytest_line)."""
    r = run("uv run pytest test/test_uc1.py test/test_uc2.py test/test_uc3.py "
            "test/test_uc4.py test/test_uc5.py -q", capture=True)
    tail = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else ""
    return r.returncode == 0, tail


def wait_drain(drain_timeout=120):
    """Wait for every queue to empty after a client finishes so nothing bleeds into the
    next one. False if it never drains in time."""
    deadline = time.time() + drain_timeout
    while time.time() < deadline:
        if queue_total() == 0:
            return True
        time.sleep(2)
    return False
