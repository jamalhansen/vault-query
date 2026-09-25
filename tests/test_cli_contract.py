"""The command-line contract: what vq, vq-fix and vq-todos print and exit with, as golden output.

A change of CLI framework must not change them. Runs the installed scripts against a fresh
copy of the sample vault (HOME points at a temp dir holding it as ~/vaults/Sample).
Re-record deliberately with RECORD_CLI_CONTRACT=1.
"""
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "sample_vault"
GOLDEN = Path(__file__).parent / "golden" / "cli_contract.json"
BIN = Path(sys.executable).parent

TODO_NOTE = """# Tasks
- [ ] overdue thing 📅 2026-01-01
- [ ] later thing 📅 2027-06-01
- [ ] not started 🛫 2027-01-01 📅 2027-02-01
- [ ] undated thing
- [x] done thing
"""
MAP = "rename_keys:\n  Status: status\nfield_values:\n  status:\n    wip: draft\nset_fields:\n  reviewed: 'no'\n"

CASES = {
    "vq-count": ["vq", "Sample", "SELECT count(*) AS n FROM notes"],
    "vq-absolute": ["vq", "{V}", "SELECT count(*) AS n FROM notes"],
    "vq-schema-csv": ["vq", "Sample", "-s", "-f", "csv"],
    "vq-json": ["vq", "Sample", "SELECT path FROM notes ORDER BY path LIMIT 3", "--format", "json"],
    "vq-dry-run": ["vq", "Sample", "--dry-run"],
    "vq-verbose": ["vq", "Sample", "-V", "SELECT count(*) FROM notes"],
    "vq-nothing-to-do": ["vq", "Sample"],
    "vq-bad-sql": ["vq", "Sample", "SELEC nope"],
    "vq-missing-vault": ["vq", "Nowhere", "SELECT 1"],
    "vq-bad-format": ["vq", "Sample", "SELECT 1", "-f", "xml"],
    "fix-lowercase-dry": ["vq-fix", "Sample", "-l"],
    "fix-map-apply": ["vq-fix", "Sample", "--map", "{T}/map.yaml", "--apply", "-V"],
    "fix-nothing": ["vq-fix", "Sample"],
    "fix-missing-map": ["vq-fix", "Sample", "-m", "{T}/none.yaml"],
    "fix-missing-vault": ["vq-fix", "Nowhere", "-l"],
    "todos-all": ["vq-todos", "Sample", "--today", "2026-09-25"],
    "todos-overdue": ["vq-todos", "Sample", "-o", "--today", "2026-09-25"],
    "todos-not-started": ["vq-todos", "Sample", "-n", "--today", "2026-09-25"],
    "todos-notes-by-file": ["vq-todos", "Sample", "-s", "notes", "--by-file", "--today", "2026-09-25"],
    "todos-json": ["vq-todos", "Sample", "-f", "json", "--today", "2026-09-25"],
    "todos-csv-archive": ["vq-todos", "Sample", "-f", "csv", "-a", "--today", "2026-09-25"],
    "todos-default-vault": ["vq-todos", "--today", "2026-09-25"],
    "todos-bad-today": ["vq-todos", "Sample", "--today", "25/09/2026"],
    "todos-bad-source": ["vq-todos", "Sample", "-s", "email"],
    "todos-missing-vault": ["vq-todos", "Nowhere"],
}
SEQUENCES = {
    "vq-db-then-reuse": [
        ["vq", "Sample", "SELECT count(*) FROM notes", "--db", "{T}/q.duckdb"],
        ["vq", "Sample", "SELECT count(*) FROM notes", "-d", "{T}/q.duckdb", "-r"],
    ],
}


def _run(argv: list[str], tmp: Path) -> dict:
    vault = tmp / "vaults" / "Sample"
    args = [a.replace("{V}", str(vault)).replace("{T}", str(tmp)) for a in argv]
    env = {**os.environ, "HOME": str(tmp), "LOCAL_FIRST_TRACKING_DB": str(tmp / "tracking.duckdb"),
           "VQ_TODOS_VAULT": "Sample"}
    proc = subprocess.run([str(BIN / args[0]), *args[1:]], capture_output=True, text=True, env=env, cwd=tmp, check=False)
    norm = lambda t: t.replace(str(tmp), "<TMP>")
    result = {"exit": proc.returncode, "stdout": norm(proc.stdout)}
    if proc.returncode != 2:  # usage errors: only the exit code is part of the contract
        result["stderr"] = re.sub(r"\d+\.\d+s", "<T>", norm(proc.stderr))
    return result


def _fresh(tmp: Path) -> Path:
    shutil.copytree(FIXTURE, tmp / "vaults" / "Sample")
    (tmp / "vaults" / "Sample" / "tasks.md").write_text(TODO_NOTE)
    (tmp / "map.yaml").write_text(MAP)
    return tmp


def test_cli_contract(tmp_path_factory):
    results = {name: _run(argv, _fresh(tmp_path_factory.mktemp(name))) for name, argv in CASES.items()}
    for name, seq in SEQUENCES.items():
        tmp = _fresh(tmp_path_factory.mktemp(name))
        results[name] = [_run(argv, tmp) for argv in seq]
    if os.environ.get("RECORD_CLI_CONTRACT"):
        GOLDEN.parent.mkdir(exist_ok=True)
        GOLDEN.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
        pytest.skip("recorded")
    golden = json.loads(GOLDEN.read_text())
    assert set(results) == set(golden)
    for name in golden:
        assert results[name] == golden[name], name
