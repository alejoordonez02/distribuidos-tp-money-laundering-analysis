"""Dynamic parser for config.yaml — all project configuration as typed functions."""

import yaml
from pathlib import Path

_cfg = yaml.safe_load((Path(__file__).parent / "config.yaml").read_text())


def transactions_path() -> str:
    return _cfg["dataset"]["transactions_path"]


def accounts_path() -> str:
    return _cfg["dataset"]["accounts_path"]


def accounts_sample_size() -> int | None:
    return _cfg["dataset"]["accounts_sample_size"]


def nclients() -> int:
    return _cfg["clients"]["nclients"]


def transactions_sample_frac() -> float:
    return _cfg["clients"]["total_sample_frac"] / _cfg["clients"]["nclients"]


def datasets_path() -> str:
    return _cfg["clients"]["datasets_path"]


def expected_responses_path() -> str:
    return _cfg["clients"]["expected_responses_path"]


def responses_path() -> str:
    return _cfg["clients"]["responses_path"]
