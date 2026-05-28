import re
import sys
from pathlib import Path

from cfg import (
    CLIENT_EXPECTED_RESPONSES_PATH,
    CLIENT_RESPONSES_PATH,
    NCLIENTS,
)

OUTPUT_DIR = Path("demo/files")

_UCS = ["UC1", "UC2", "UC3", "UC4", "UC5"]


def _norm_uc1_actual(line: str) -> str:
    m = re.match(r"origin: (\d+)-(\S+)\s+destination: (\d+)-(\S+)\s+amount: (\S+)", line)
    if not m:
        return line
    return f"origin: {m[1]}-{m[2]}  destination: {m[3]}-{m[4]}  amount: {m[5]}"


def _norm_uc2_actual(line: str) -> str:
    m = re.match(r"bank_id: (\S+)\s+account: (\S+)\s+bank_name: (.*?)\s+amount: (\S+)$", line.strip())
    if not m:
        return line
    return f"bank_id: {m[1]}  account: {m[2]}  bank_name: {m[3]}  amount: {m[4]}"


def _norm_uc3_actual(line: str) -> str:
    m = re.match(r"bank_id: (\S+)\s+account: (\S+)\s+payment_format: (.*?)\s+amount: (\S+)$", line.strip())
    if not m:
        return line
    return f"bank_id: {m[1]}  account: {m[2]}  payment_format: {m[3]}  amount: {m[4]}"


def _norm_uc4_actual(line: str) -> str:
    m = re.match(r"bank: (\S+)\s+account: (\S+)", line.strip())
    if not m:
        return line
    return f"bank: {m[1]}  account: {m[2]}"


def _norm_uc5_actual(line: str) -> str:
    return line.strip()


_ACTUAL_NORMS = {
    "UC1": _norm_uc1_actual,
    "UC2": _norm_uc2_actual,
    "UC3": _norm_uc3_actual,
    "UC4": _norm_uc4_actual,
    "UC5": _norm_uc5_actual,
}


def parse_responses(path: str) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {uc: [] for uc in _UCS}
    current = None
    with open(path) as f:
        for raw in f:
            line = raw.rstrip("\n")
            m = re.match(r"--- (UC\d+) ---", line)
            if m:
                current = m.group(1)
                continue
            if current and line.strip():
                sections[current].append(_ACTUAL_NORMS[current](line))
    return sections


def parse_expected_uc1(path: str) -> list[str]:
    lines = []
    with open(path) as f:
        f.readline()
        for raw in f:
            parts = raw.rstrip("\n").split(",")
            _, fb, fa, tb, ta, amt = parts
            lines.append(f"origin: {fb}-{fa}  destination: {tb}-{ta}  amount: {amt}")
    return lines


def parse_expected_uc2(path: str) -> list[str]:
    lines = []
    with open(path) as f:
        f.readline()
        for raw in f:
            parts = raw.rstrip("\n").split(",", 4)
            _, bank_id, account, bank_name, amount = parts
            lines.append(f"bank_id: {bank_id}  account: {account}  bank_name: {bank_name}  amount: {amount}")
    return lines


def parse_expected_uc3(path: str) -> list[str]:
    lines = []
    with open(path) as f:
        f.readline()
        for raw in f:
            parts = raw.rstrip("\n").split(",")
            _, bank_id, account, fmt, amount = parts
            lines.append(f"bank_id: {bank_id}  account: {account}  payment_format: {fmt}  amount: {amount}")
    return lines


def parse_expected_uc4(path: str) -> list[str]:
    lines = []
    with open(path) as f:
        f.readline()
        for raw in f:
            parts = raw.rstrip("\n").split(",")
            if len(parts) < 3:
                continue
            _, bank, account = parts
            lines.append(f"bank: {bank}  account: {account}")
    return lines


def parse_expected_uc5(path: str) -> list[str]:
    with open(path) as f:
        return [f"count: {f.read().strip()}"]


def write_txt(path: Path, sections: dict[str, list[str]]):
    with open(path, "w") as f:
        for uc in _UCS:
            f.write(f"=== {uc} ===\n")
            for line in sorted(sections[uc]):
                f.write(line + "\n")
            f.write("\n")


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for n in range(NCLIENTS):
        resp_path = CLIENT_RESPONSES_PATH + f"responses_{n}.csv"
        try:
            resp_sections = parse_responses(resp_path)
        except FileNotFoundError:
            print(f"[!] Client {n}: {resp_path} not found — skipping", file=sys.stderr)
            continue

        expected_sections = {
            "UC1": parse_expected_uc1(CLIENT_EXPECTED_RESPONSES_PATH + f"uc1_{n}.csv"),
            "UC2": parse_expected_uc2(CLIENT_EXPECTED_RESPONSES_PATH + f"uc2_{n}.csv"),
            "UC3": parse_expected_uc3(CLIENT_EXPECTED_RESPONSES_PATH + f"uc3_{n}.csv"),
            "UC4": parse_expected_uc4(CLIENT_EXPECTED_RESPONSES_PATH + f"uc4_{n}.csv"),
            "UC5": parse_expected_uc5(CLIENT_EXPECTED_RESPONSES_PATH + f"uc5_{n}.csv"),
        }

        out_resp = OUTPUT_DIR / f"CLIENT_{n}_RESPONSES.txt"
        out_exp = OUTPUT_DIR / f"CLIENT_{n}_EXPECTED.txt"
        write_txt(out_resp, resp_sections)
        write_txt(out_exp, expected_sections)
        print(f"Client {n} → {out_resp.name}  {out_exp.name}")

    print(f"\nFiles written to {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
