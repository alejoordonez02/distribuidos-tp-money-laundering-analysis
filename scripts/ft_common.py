"""Shared cluster-driving helpers for the e2e harnesses (ft_e2e, scalability_e2e,
ft_perf_bench).

They bring the cluster up once, run fresh clients through it, detect stalls by queue
progress and drain between clients; keeping that here means they read docker state the same
way. The topology/tier/cluster-bring-up helpers are shared too so the scalability and
performance benchmarks don't each re-implement them. These scripts live in scripts/ and run
as files, so a sibling `from ft_common import` resolves directly.
"""

import os
import subprocess
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPOSE = "docker-compose.yaml"
CFG = os.path.join(ROOT, "scripts/cfg.py")
TOPOLOGY = os.path.join(ROOT, "scripts/gen_compose/src/topology.py")
DATASETS = os.path.join(ROOT, "datasets")


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


def expected_controllers():
    """Flat list of controller container names (the warm set expected to stay running)."""
    infra = {"rabbitmq", "gateway", "supervisor", "chaos"}
    services = out(f"docker compose -f {COMPOSE} config --services").splitlines()
    return [s.strip() for s in services
            if s.strip() and s.strip() not in infra and not s.strip().startswith("client")]


def running_names():
    return set(out('docker ps --format "{{.Names}}"').split())


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
    """Run the per-UC oracle comparison, returning (ok, summary_with_failed_ucs)."""
    r = run("uv run pytest test/test_uc1.py test/test_uc2.py test/test_uc3.py "
            "test/test_uc4.py test/test_uc5.py -q -rf", capture=True)
    lines = r.stdout.strip().splitlines() if r.stdout.strip() else []
    failed = [l.strip() for l in lines if "FAILED" in l]
    tail = lines[-1] if lines else ""
    if failed:
        tail = " | ".join(failed) + " || " + tail
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


TIERS = {
    "perfect": ("datasets/perfect_sample.csv", "datasets/perfect_sample_accounts.csv", None),
    "small": ("datasets/LI-Small_Trans.csv", "datasets/LI-Small_accounts.csv", "small"),
    "medium": ("datasets/HI-Medium_Trans.csv", "datasets/HI-Medium_accounts.csv", "medium"),
    "large": ("datasets/HI-Large_Trans.csv", "datasets/HI-Large_accounts.csv", "large"),
}

TIER_CAP = {
    "perfect": int(os.getenv("FT_CAP_PERFECT", "300")),
    "small": int(os.getenv("FT_CAP_SMALL", "900")),
    "medium": int(os.getenv("FT_CAP_MEDIUM", "1800")),
    "large": int(os.getenv("FT_CAP_LARGE", "4500")),
}

TOPOLOGY_KEYS = [
    "DEFAULT_FILTERS",
    "UC2_MAX_AMOUNT_GROUP_BYS", "UC2_MAX_AMOUNT_AGGREGATES",
    "UC2_BANK_NAMES_GROUP_BYS", "UC2_BANK_NAMES_AGGREGATES", "UC2_MERGES",
    "UC3_GROUP_BYS", "UC3_AGGREGATES", "UC3_MERGES", "UC3_FILTERS",
    "UC4_COMPUTE_GRAPHS", "UC4_AGGREGATE_GRAPHS", "UC4_DEGREE_AGGREGATES",
    "UC4_PRUNES", "UC4_COUNT_PATHS", "UC4_PATHS_AGGREGATES",
    "UC5_CONVERTERS", "UC5_AMOUNT_FILTERS", "UC5_COUNT_GROUP_BYS",
]

CURRENT = {
    "DEFAULT_FILTERS": 2,
    "UC2_MAX_AMOUNT_GROUP_BYS": 1, "UC2_MAX_AMOUNT_AGGREGATES": 1,
    "UC2_BANK_NAMES_GROUP_BYS": 1, "UC2_BANK_NAMES_AGGREGATES": 1, "UC2_MERGES": 2,
    "UC3_GROUP_BYS": 1, "UC3_AGGREGATES": 1, "UC3_MERGES": 2, "UC3_FILTERS": 1,
    "UC4_COMPUTE_GRAPHS": 2, "UC4_AGGREGATE_GRAPHS": 1, "UC4_DEGREE_AGGREGATES": 1,
    "UC4_PRUNES": 2, "UC4_COUNT_PATHS": 2, "UC4_PATHS_AGGREGATES": 2,
    "UC5_CONVERTERS": 1, "UC5_AMOUNT_FILTERS": 1, "UC5_COUNT_GROUP_BYS": 1,
}

JOIN_PARTITION_BLOCK = (
    "\nJOIN_PARTITION = [\n"
    "    [1],\n"
    "    [3],\n"
    "    [2, 4, 5],\n"
    "]\n"
)


def variant(**overrides):
    v = dict(CURRENT)
    v.update(overrides)
    return v


def render_topology(values):
    """Render a topology.py from a constant set, preserving JOIN_PARTITION."""
    lines = [f"DEFAULT_FILTERS = {values['DEFAULT_FILTERS']}", "", "# UC2"]
    lines += [f"{k} = {values[k]}" for k in TOPOLOGY_KEYS[1:6]]
    lines += ["", "# UC3"] + [f"{k} = {values[k]}" for k in TOPOLOGY_KEYS[6:10]]
    lines += ["", "# UC4"] + [f"{k} = {values[k]}" for k in TOPOLOGY_KEYS[10:16]]
    lines += ["", "# UC5"] + [f"{k} = {values[k]}" for k in TOPOLOGY_KEYS[16:19]]
    lines += [""]
    return "\n".join(lines) + JOIN_PARTITION_BLOCK


def render_cfg(trans, accts):
    """Render scripts/cfg.py for a tier (NCLIENTS stays 1 so the cached oracle applies)."""
    return (
        f'TRANSACTIONS_PATH = "{trans}"\n'
        f'ACCOUNTS_PATH = "{accts}"\n'
        "ACCOUNTS_SAMPLE_SIZE = None\n\n"
        "NCLIENTS = 1\n"
        "TRANSACTIONS_SAMPLE_FRAC: float = 1 / NCLIENTS\n"
        'CLIENT_DATASETS_PATH = "datasets/"\n'
        'CLIENT_EXPECTED_RESPONSES_PATH = "test/expected_responses/"\n\n'
        'CLIENT_RESPONSES_PATH = "responses/"\n'
    )


def write_topology(values):
    with open(TOPOLOGY, "w") as f:
        f.write(render_topology(values))


def setup_tier(tier):
    """Point cfg.py at the tier, set the relative symlink and install the cached oracle (or
    regenerate it for a tier with no cache). Raises on a failed oracle generation."""
    trans, accts, cache = TIERS[tier]
    with open(CFG, "w") as f:
        f.write(render_cfg(trans, accts))
    link = os.path.join(DATASETS, "transactions_0.csv")
    if os.path.lexists(link):
        os.remove(link)
    os.symlink(os.path.basename(trans), link)
    if cache is None:
        r = run("PYTHONPATH=src uv run scripts/gen_input_output.py", capture=True)
        if r.returncode != 0:
            raise RuntimeError(f"gen_input_output failed for {tier}: {r.stdout[-400:]}")
    else:
        src = os.path.join(ROOT, "test/expected_cache", cache)
        dst = os.path.join(ROOT, "test/expected_responses")
        os.makedirs(dst, exist_ok=True)
        run(f"cp {src}/*.csv {dst}/", capture=True)


def gen_compose(checkpoint_every=None):
    env = f"CHECKPOINT_EVERY={checkpoint_every} " if checkpoint_every is not None else ""
    run(f"{env}uv run -m scripts.gen_compose.gen_compose {COMPOSE}", capture=True)


def teardown():
    run(f"docker compose -f {COMPOSE} down --remove-orphans -t 5", capture=True)


def bring_up_cluster(do_build=False, checkpoint_every=None, startup_grace=25):
    """Fresh cluster for a new config: tear the old one down, regenerate the compose from the
    already-written topology, optionally build the images, wipe state and bring it up."""
    teardown()
    gen_compose(checkpoint_every)
    if do_build:
        run(f"docker compose -f {COMPOSE} build", capture=True)
    clean_all_state()
    compose_up_cluster()
    time.sleep(startup_grace)


_SNAPSHOT = {}


def snapshot_files():
    for path in (CFG, TOPOLOGY):
        with open(path) as f:
            _SNAPSHOT[path] = f.read()


def restore_files():
    for path, content in _SNAPSHOT.items():
        with open(path, "w") as f:
            f.write(content)
