import sys
from pathlib import Path

# Each service runs with its own source dir on PYTHONPATH (so it imports its node and fns
# as top-level modules, e.g. `ring_aggregate`, `aggregate_fns`). Mirror that here so the
# service-specific ring nodes can be unit-tested. The fn package names are unique per
# service; only `strategies`/`main` collide and neither is imported under test.
_SRC = Path(__file__).resolve().parent.parent / "src"
for _svc in ("aggregate", "merge", "group_by"):
    _p = str(_SRC / _svc)
    if _p not in sys.path:
        sys.path.insert(0, _p)
