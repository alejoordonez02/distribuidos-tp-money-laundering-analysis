import sys
from pathlib import Path

# Each service runs with its own source dir on PYTHONPATH, importing its node and fns as top-level modules (e.g. `ring_aggregate`, `aggregate_fns`); mirror that here so they can be unit-tested. Only `strategies`/`main` collide, and neither is imported under test.
_SRC = Path(__file__).resolve().parent.parent / "src"
for _svc in ("aggregate", "merge", "group_by"):
    _p = str(_SRC / _svc)
    if _p not in sys.path:
        sys.path.insert(0, _p)
