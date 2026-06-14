#!/bin/bash
# Resilience demo: run a dataset under the chaos monkey and check exactly-once.
#
# Detection is heartbeat-based; the supervisor revives killed nodes via Docker
# (node restart policy is OFF so the supervisor is the sole reviver). The run
# passes if verify is still 5/5 after the carnage.
#
# Defaults: medium, kill 1-8 nodes every 4s, supervisor revives every 4s.
# Override via env, e.g.:  DATASET=small CHAOS_INTERVAL=1 bash scripts/resilience_demo.sh
set -u
cd "$(dirname "$0")/.." || exit 1
LOG(){ echo ">>> [$(date +%H:%M:%S)] $*"; }

DATASET="${DATASET:-medium}"
case "$DATASET" in
  small)  TRANS=LI-Small_Trans.csv;  ACCTS=LI-Small_accounts.csv ;;
  medium) TRANS=HI-Medium_Trans.csv; ACCTS=HI-Medium_accounts.csv ;;
  large)  TRANS=HI-Large_Trans.csv;  ACCTS=HI-Large_accounts.csv ;;
  *) echo "unknown DATASET=$DATASET (use small|medium|large)"; exit 1 ;;
esac

# chaos / supervisor knobs (compose reads these via ${VAR:-default} substitution)
export CHAOS_ENABLED=1
export CHAOS_INTERVAL="${CHAOS_INTERVAL:-4}"
export CHAOS_KILLS_MIN="${CHAOS_KILLS_MIN:-1}"
export CHAOS_KILLS_MAX="${CHAOS_KILLS_MAX:-8}"
export CHAOS_START_DELAY="${CHAOS_START_DELAY:-15}"
export REVIVE_INTERVAL="${REVIVE_INTERVAL:-4}"
export HEARTBEAT_TIMEOUT="${HEARTBEAT_TIMEOUT:-6}"
export HEARTBEAT_INTERVAL="${HEARTBEAT_INTERVAL:-2}"

LOG "dataset=$DATASET  chaos: ${CHAOS_KILLS_MIN}-${CHAOS_KILLS_MAX} kills / ${CHAOS_INTERVAL}s  revive: ${REVIVE_INTERVAL}s"

# point cfg + per-client symlink at the dataset; reuse the cached oracle (no regen)
python3 - "datasets/$TRANS" "datasets/$ACCTS" <<'PY'
import sys
trans, accts = sys.argv[1], sys.argv[2]
open("scripts/cfg.py", "w").write(
f'''TRANSACTIONS_PATH = "{trans}"
ACCOUNTS_PATH = "{accts}"
ACCOUNTS_SAMPLE_SIZE = None

NCLIENTS = 1
TRANSACTIONS_SAMPLE_FRAC: float = 1 / NCLIENTS
CLIENT_DATASETS_PATH = "datasets/"
CLIENT_EXPECTED_RESPONSES_PATH = "test/expected_responses/"

CLIENT_RESPONSES_PATH = "responses/"
''')
PY
ln -sf "$TRANS" datasets/transactions_0.csv
rm -f test/expected_responses/uc*_0.csv
cp test/expected_cache/$DATASET/uc*_0.csv test/expected_responses/ || { echo "no expected_cache for $DATASET"; exit 1; }
rm -rf state/* responses/*.csv 2>/dev/null; mkdir -p responses

LOG "gen_compose + up --build"
uv run -m scripts.gen_compose.gen_compose docker-compose.yaml || exit 1
docker compose up --build --remove-orphans --detach 2>&1 | tail -2 || exit 1

start=$(date +%s)
LOG "running under chaos; waiting for client (max 50m)"
for i in $(seq 1 1000); do
  [ "$(docker ps --filter name=client_ --filter status=running -q | wc -l)" -eq 0 ] && { sleep 3; break; }
  if [ $((i % 10)) -eq 0 ]; then
    q=$(docker exec rabbitmq rabbitmqctl list_queues messages --quiet 2>/dev/null | awk '{s+=$1} END{print s}')
    LOG "  +$(( $(date +%s)-start ))s  up=$(docker ps -q | wc -l)  queue=${q:-?}  revives=$(docker logs supervisor 2>&1 | grep -c reviving)"
  fi
  sleep 3
done

ec=$(docker inspect -f '{{.State.ExitCode}}' client_0 2>/dev/null)
elapsed=$(( $(date +%s)-start ))
revives=$(docker logs supervisor 2>&1 | grep -c reviving)
LOG "client exit=$ec  elapsed=${elapsed}s  supervisor_revives=$revives"

CHAOS_ENABLED=0 docker compose up -d --force-recreate --no-deps chaos >/dev/null 2>&1
LOG "verify (still 5/5 after the chaos?)"
uv run pytest test/test_uc1.py test/test_uc2.py test/test_uc3.py test/test_uc4.py test/test_uc5.py -q 2>&1 | tail -3
rc=${PIPESTATUS[0]}
LOG "RESULT dataset=$DATASET verify_rc=$rc elapsed=${elapsed}s revives=$revives"

docker compose down -t 5 >/dev/null 2>&1
git checkout scripts/cfg.py >/dev/null 2>&1
LOG "DONE"
